"""
silver_processed_dag.py — Silver 적재 + Gold 집계 DAG

Bronze raw → Silver processed_events(MERGE) → Gold KPI 집계를 Airflow가 매일(@daily)
Spark standalone 클러스터에 제출해 실행한다.

구조:
  silver_processed_merge >> gold_aggregate >> compact_gold   (데이터 경로 + gold 압축)
  silver_processed_merge >> compact_silver                   (silver 압축, 분기 trailing)

  - compaction을 쓰기 직후 인라인(silver/gold) — write-driven이라 여기가 맞음.
    (expire/orphan 같은 time-driven GC는 별도 iceberg_maintenance DAG에서 주기로.)
  - compaction은 데이터 경로(silver>>gold)에서 분기한 trailing이라, 압축 실패가
    gold/데이터를 막지 않는다. compact_silver는 silver_merge 끝난 뒤라 동시 writer 없음(OCC 없음).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# 이미 캐시된 Iceberg + AWS jar를 직접 지정 (--packages 런타임 다운로드 회피 → 충돌·실패 방지).
# infra_ivy-cache 볼륨을 /opt/cache로 마운트해 사용.
SPARK_JARS = (
    "/opt/cache/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar,"
    "/opt/cache/jars/org.apache.iceberg_iceberg-aws-bundle-1.11.0.jar"
)

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_processed",
    description="Bronze raw → Silver processed_events (MERGE INTO, sliding window)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,           # 과거 미실행 구간 채우기 안 함
    max_active_runs=1,       # 동시 실행 금지 (jar/리소스 충돌 방지)
    default_args=default_args,
    tags=["silver", "iceberg"],
    # 트리거 시 조절 가능. 기본(스케줄 실행) = 전체 window, hour=-1(전체).
    # 검증/개발 시 수동 트리거에서 {"window_days":1,"hour":12} 같이 작은 슬라이스로.
    params={"window_days": 7, "hour": -1},
) as dag:

    silver_merge = SparkSubmitOperator(
        task_id="silver_processed_merge",
        conn_id="spark_default",                       # spark://spark-master:7077
        application="/opt/spark/work-dir/silver_processed.py",  # 마운트된 최신 코드
        application_args=[
            "--window-days", "{{ params.window_days }}",
            "--hour", "{{ params.hour }}",
        ],
        jars=SPARK_JARS,
        conf={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "4g",
            # OOM 안전벨트(단일 worker): 동시 task 줄이고(cores↓) 셔플 파티션 잘게(메모리 분산)
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": "400",
        },
        verbose=False,
    )

    # Silver 성공 후 Gold KPI 집계 (updated_at 기반 증분 — 변경된 event_date 파티션만 재계산)
    gold_aggregate = SparkSubmitOperator(
        task_id="gold_aggregate",
        conn_id="spark_default",
        application="/opt/spark/work-dir/gold_aggregations.py",
        application_args=["--lookback-days", "3"],
        jars=SPARK_JARS,
        conf={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "4g",
        },
        verbose=False,
    )

    # ── 인라인 compaction (write-driven) — 쓰기 직후 그 레이어만 압축 ──────────────
    # 로직은 iceberg_maintenance.py 단일 모듈, DAG는 cadence(언제)만 wiring.
    # compact는 S3FileIO라 hadoop-aws 불필요 → 기존 SPARK_JARS로 충분.
    def _compact(task_id: str, layer: str) -> SparkSubmitOperator:
        return SparkSubmitOperator(
            task_id=task_id,
            conn_id="spark_default",
            application="/opt/spark/work-dir/iceberg_maintenance.py",
            application_args=["--layer", layer, "--ops", "compact"],
            jars=SPARK_JARS,
            conf={"spark.driver.memory": "2g", "spark.executor.memory": "4g"},
            verbose=False,
        )

    compact_silver = _compact("compact_silver", "silver")
    compact_gold = _compact("compact_gold", "gold")

    # 데이터 경로(silver>>gold)는 그대로, compaction은 분기 trailing(non-blocking).
    silver_merge >> gold_aggregate >> compact_gold
    silver_merge >> compact_silver
