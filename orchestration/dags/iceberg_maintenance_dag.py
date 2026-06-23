"""
iceberg_maintenance_dag.py — Iceberg 주기 GC DAG (time-driven 유지보수)

유지보수를 cadence별로 분리한 뒤, 이 DAG는 **time-driven 청소만** 담당한다:
  - compact_bronze : Bronze compaction (streaming이라 '쓰기 끝' 시점이 없어 인라인 불가 → 주기로)
  - expire         : 오래된 스냅샷 제거 (전 레이어 bronze+silver+gold)
  - orphan         : 고아 파일 제거 (전 레이어)

  → compact_bronze >> expire >> orphan

silver/gold의 compaction(write-driven)은 여기가 아니라 메달리온 DAG(silver_processed)에
**쓰기 직후 인라인**으로 들어가 있다. (연산 로직은 iceberg_maintenance.py 단일 모듈, DAG는 cadence만 wiring)

스케줄:
  매일 04:00. expire/orphan은 시간 기반이라 일 1회로 충분(매번 돌리면 헛바퀴, orphan은 매번 전체 LIST).
  Bronze compaction은 24/7 streaming과 동시라 시간대 분리가 무의미 → partial-progress로 OCC 처리.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# 이미 캐시된 Iceberg + AWS jar를 직접 지정 (--packages 런타임 다운로드 회피).
# silver_processed_dag와 동일하게 infra_ivy-cache 볼륨(/opt/cache)을 사용.
# hadoop-aws + aws-java-sdk-bundle은 remove_orphan_files가 s3:// 위치를 Hadoop
# FileSystem(S3A)으로 리스팅하는 데 필요하다 (적재 잡엔 불필요했던 추가 의존성).
SPARK_JARS = (
    "/opt/cache/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar,"
    "/opt/cache/jars/org.apache.iceberg_iceberg-aws-bundle-1.11.0.jar,"
    "/opt/cache/jars/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
    "/opt/cache/jars/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar"
)

APP = "/opt/spark/work-dir/iceberg_maintenance.py"
SPARK_CONF = {"spark.driver.memory": "2g", "spark.executor.memory": "4g"}

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _op(task_id: str, layer: str, op: str, *extra: str) -> SparkSubmitOperator:
    """지정 레이어에 단일 유지보수 연산(op)을 실행하는 태스크 생성."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",                       # spark://spark-master:7077
        application=APP,                               # 마운트된 최신 코드
        application_args=["--layer", layer, "--ops", op, *extra],
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        verbose=False,
    )


with DAG(
    dag_id="iceberg_maintenance",
    description="Iceberg 주기 GC — bronze compaction + expire/orphan(전 레이어)",
    schedule="0 4 * * *",     # 매일 04:00. time-driven GC는 일 1회로 충분
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,        # 동시 실행 금지 (jar/리소스/OCC 충돌 방지)
    default_args=default_args,
    tags=["maintenance", "iceberg", "gc"],
) as dag:

    # 순서: bronze 압축(새 스냅샷) → expire(구 스냅샷 제거) → orphan(고아 파일 회수)
    compact_bronze = _op("compact_bronze", "bronze", "compact")   # streaming이라 주기로
    # 고빈도(15분) 메달리온이라 스냅샷이 빨리 쌓임 → 보존 2일로 단축(time-travel 창도 2일)
    expire = _op("expire", "all", "expire", "--snapshot-retention-days", "2")
    orphan = _op("orphan", "all", "orphan")

    compact_bronze >> expire >> orphan
