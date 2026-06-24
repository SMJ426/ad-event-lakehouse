"""
medallion_dag.py — 메달리온 변환 DAG (Silver MERGE + Gold 집계 + 인라인 compaction)

이름 정정: 이전 `silver_processed`는 실제론 silver뿐 아니라 gold·compaction까지 도는
메달리온 변환 전체라, dag_id를 `medallion`으로 바꿨다. (Bronze 스트리밍·GC는 별도)

구조:
  silver_merge >> gold_aggregate >> compact_gold   (데이터 경로 + gold 압축)
  silver_merge >> compact_silver                   (silver 압축, 분기 trailing)

  - gold는 silver 직후 같은 DAG에서 유기적으로 실행 → 대시보드(Gold)도 같은 주기로 신선.
  - compaction은 write-driven이라 쓰기 직후 인라인. 데이터 경로에서 분기한 trailing이라
    압축 실패가 gold/데이터를 막지 않음(non-blocking).

스케줄(near-real-time):
  15분(*/15). Silver는 incremental(Bronze ingested_at 최근 N시간)로 읽어 매 실행 전체 재독을 피한다.
  쓰기는 COW 유지(현 스케일 충분, MOR는 스케일 대응). 잦은 쓰기로 스냅샷이 빨리 쌓이므로
  GC DAG의 expire 보존을 짧게 잡는다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_defaults import ICEBERG_JARS as SPARK_JARS, SPARK_CONF  # 공통 jar/conf
from slack_alert import slack_failure_callback                    # 실패 시 Slack 경보

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": slack_failure_callback,
}

with DAG(
    dag_id="medallion",
    description="Silver MERGE + Gold 집계 + 인라인 compaction (15분 incremental)",
    schedule="*/15 * * * *",   # 15분 near-real-time
    start_date=datetime(2026, 1, 1),
    catchup=False,           # 과거 미실행 구간 채우기 안 함
    max_active_runs=1,       # 동시 실행 금지 (jar/리소스 충돌 방지)
    default_args=default_args,
    tags=["medallion", "silver", "gold", "iceberg"],
    # incremental: Bronze ingested_at 최근 N시간만. (수동 트리거로 조절 가능)
    params={"lookback_hours": 3},
) as dag:

    silver_merge = SparkSubmitOperator(
        task_id="silver_merge",
        conn_id="spark_default",                       # spark://spark-master:7077
        application="/opt/spark/work-dir/silver_processed.py",  # 마운트된 최신 코드
        application_args=["--lookback-hours", "{{ params.lookback_hours }}"],
        jars=SPARK_JARS,
        conf={
            **SPARK_CONF,
            # OOM 안전벨트(단일 worker): 동시 task 줄이고(cores↓) 셔플 파티션 잘게(메모리 분산)
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": "400",
        },
        verbose=False,
    )

    # Silver 직후 Gold KPI 집계 (updated_at 기반 증분 — 변경된 event_date 파티션만 재계산)
    gold_aggregate = SparkSubmitOperator(
        task_id="gold_aggregate",
        conn_id="spark_default",
        application="/opt/spark/work-dir/gold_aggregations.py",
        application_args=["--lookback-days", "3"],
        jars=SPARK_JARS,
        conf=SPARK_CONF,
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
            conf=SPARK_CONF,
            verbose=False,
        )

    compact_silver = _compact("compact_silver", "silver")
    compact_gold = _compact("compact_gold", "gold")

    # 데이터 경로(silver>>gold)는 그대로, compaction은 분기 trailing(non-blocking).
    silver_merge >> gold_aggregate >> compact_gold
    silver_merge >> compact_silver
