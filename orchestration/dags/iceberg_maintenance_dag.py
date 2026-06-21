"""
iceberg_maintenance_dag.py — Iceberg 테이블 유지보수 자동화 DAG

Bronze·Silver Iceberg 테이블의 유지보수 잡(code/pipelines/iceberg_maintenance.py)을
Airflow가 매일 Spark standalone 클러스터에 제출해 실행한다.

구조:
  Airflow(SparkSubmitOperator) → spark://spark-master:7077 (client 모드)
    → compact >> expire >> orphan 순서로 실행
    → Bronze 4테이블 + Silver 1테이블의 small file·구 스냅샷·고아 파일 정리

스케줄/시간대 분리:
  silver_processed(@daily, 자정 기준)와 다른 시각(04:00)에 돌려, Silver 배치 MERGE와
  같은 파티션을 동시에 건드리는 상황(OCC 충돌)을 줄인다. Bronze 스트리밍은 상시 쓰므로
  compaction은 partial-progress, orphan은 older_than으로 안전 처리한다(잡 내부 정책).
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


def _op(task_id: str, op: str) -> SparkSubmitOperator:
    """단일 유지보수 연산(op)을 전체 레이어에 실행하는 태스크 생성."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id="spark_default",                       # spark://spark-master:7077
        application=APP,                               # 마운트된 최신 코드
        application_args=["--layer", "{{ params.layer }}", "--ops", op],
        jars=SPARK_JARS,
        conf=SPARK_CONF,
        verbose=False,
    )


with DAG(
    dag_id="iceberg_maintenance",
    description="Iceberg compaction / expire_snapshots / remove_orphan_files 자동화",
    schedule="0 4 * * *",     # 매일 04:00 — silver_processed(자정)와 시간대 분리
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,        # 동시 실행 금지 (jar/리소스/OCC 충돌 방지)
    default_args=default_args,
    tags=["maintenance", "iceberg"],
    # 트리거 시 조절 가능. 기본 = 전체 레이어(bronze+silver).
    params={"layer": "all"},
) as dag:

    # 순서 보장: compaction(새 스냅샷 생성) → expire(구 스냅샷 제거) → orphan(고아 파일 회수)
    compact = _op("compact", "compact")
    expire = _op("expire", "expire")
    orphan = _op("orphan", "orphan")

    compact >> expire >> orphan
