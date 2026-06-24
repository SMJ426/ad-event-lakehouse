"""
health_check_dag.py — 파이프라인 헬스체크 잡 (pipeline_health.py 주기 실행 + 실패 시 Slack)

매시간 Iceberg 메타테이블 기반 헬스체크(pipeline_health.py)를 돌린다. FAIL(exit 1)이면 태스크가
빨개지고 on_failure_callback이 Slack 경보를 보낸다.

운영 가시성 = 푸시(이 알림) + 풀(대시보드 운영 탭). 평소엔 조용, 문제 시 Slack 핑(로그 링크).
잡 주기(매시간)는 운영자의 대시보드 글랜스 주기(수시)와 별개다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_defaults import ICEBERG_JARS, SPARK_CONF   # 공통 jar/conf
from slack_alert import slack_failure_callback        # 실패 시 Slack 경보

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": slack_failure_callback,    # FAIL(exit 1)·크래시 시 Slack
}

with DAG(
    dag_id="health_check",
    description="Iceberg 메타테이블 헬스체크 (pipeline_health.py) — FAIL 시 Slack 경보",
    schedule="0 * * * *",        # 매시간. read-only라 가벼워 자주 돌려 문제를 빨리 잡는다.
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["health", "observability"],
) as dag:

    # pipeline_health.py: 8종 체크 자동 판정. FAIL 있으면 exit 1 → 태스크 실패 → 콜백 Slack.
    # 임계값은 스크립트 기본값 사용(--freshness-min 30 / --min-file-mb 32 / --max-snapshots 200).
    pipeline_health = SparkSubmitOperator(
        task_id="pipeline_health",
        conn_id="spark_default",                       # spark://spark-master:7077
        application="/opt/spark/work-dir/pipeline_health.py",
        jars=ICEBERG_JARS,
        conf=SPARK_CONF,
        verbose=False,
    )
