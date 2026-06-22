"""
silver_processed_dag.py — Silver 적재 + Gold 집계 DAG

Bronze raw → Silver processed_events(MERGE) → Gold KPI 집계를 Airflow가 매일(@daily)
Spark standalone 클러스터에 제출해 실행한다.

구조:
  Airflow(SparkSubmitOperator) → spark://spark-master:7077 (client 모드)
    silver_processed_merge : Bronze(S3) 읽고 → Silver MERGE INTO (멱등)
        ↓ (Silver 성공 후에만)
    gold_aggregate         : Silver → Gold KPI 집계 (updated_at 기반 증분)

Gold는 Silver 하류에 둬서 불완전한 Silver로 KPI를 오산하지 않게 한다
(architecture "Silver 완료 후 즉시"와 일치).
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

    silver_merge >> gold_aggregate
