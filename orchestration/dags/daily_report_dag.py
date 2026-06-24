"""
daily_report_dag.py — 데일리 KPI 리포트 잡 (daily_report.py를 매일 실행 → Slack 봇 게시)

매일 아침 Gold 집계로 KPI 요약(WoW/MoM 동요일 비교 + 추세 차트)을 Slack 채널에 푸시한다.
실패 경보(slack_alert)와 달리 '정상 운영 요약'을 능동 푸시하는 잡. 리포트 잡 자체가 실패하면
on_failure_callback으로 Slack 경보가 간다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_defaults import ICEBERG_JARS, SPARK_CONF   # 공통 jar/conf
from slack_alert import slack_failure_callback        # 잡 실패 시 Slack 경보

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": slack_failure_callback,
}

with DAG(
    dag_id="daily_report",
    description="Gold KPI 데일리 리포트 (WoW/MoM + 추세 차트) → Slack 봇",
    schedule="0 0 * * *",        # 매일 09:00 KST (00:00 UTC), tunable
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["report", "observability"],
) as dag:

    daily_report = SparkSubmitOperator(
        task_id="daily_report",
        conn_id="spark_default",                       # spark://spark-master:7077
        application="/opt/spark/work-dir/daily_report.py",
        jars=ICEBERG_JARS,
        conf=SPARK_CONF,
        verbose=False,
    )
