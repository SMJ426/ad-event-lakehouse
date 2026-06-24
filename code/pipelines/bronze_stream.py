"""
bronze_stream.py — Kafka → S3 Iceberg(Bronze) 적재 Spark Structured Streaming 잡

역할:
  로컬 Kafka의 4개 토픽(ad-requests/impressions/clicks/conversions)을 구독하여
  메시지를 변환 없이 raw 그대로 S3 Iceberg Bronze 테이블에 적재한다.

Bronze = raw 원칙:
  Kafka value(JSON 문자열)를 파싱하지 않고 그대로 저장한다.
  같은 토픽에 dummy(AdEvent) / criteo(CriteoRawEvent) 스키마가 섞여 있으므로 파싱하면 충돌한다. 
  스키마 통일·파싱은 Silver 레이어의 역할이다.

저장 메타:
  Kafka 메타데이터(topic/partition/offset/timestamp)를 함께 저장해 중복 추적·재처리·신선도 측정에 활용한다.

실행:
  spark-submit --packages ... bronze_stream.py
  (필요 환경변수: S3_BUCKET, KAFKA_BOOTSTRAP_SERVERS, AWS_REGION)
"""

import json
import os
import socket
import time
import urllib.request
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, hour
from pyspark.storagelevel import StorageLevel

try:  # PySpark 3.5+
    from pyspark.errors import StreamingQueryException
except ImportError:  # 구버전 호환
    from pyspark.sql.utils import StreamingQueryException

from spark_common import S3_BUCKET, CATALOG, build_spark  # 공통 Spark 설정

# ── 환경 설정 ────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
# 배치당 읽는 메시지 수. 소비속도 = 이 값 / trigger(60s). 인프라단에서 조절.
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000")

# ── 재시작/알림 설정 ─────────────────────────────────────────────────────────
# 스트리밍이 죽으면 곧장 포기하지 않고, 잡 안에서 체크포인트 기준으로 다시 시작한다(최대 N회).
# 재시작 사이 대기시간(backoff)은 지수로 늘려 지속 오류 시 과부하를 막는다.
MAX_STREAM_RESTARTS = int(os.environ.get("MAX_STREAM_RESTARTS", "5"))
RESTART_BACKOFF_SECONDS = int(os.environ.get("RESTART_BACKOFF_SECONDS", "10"))  # 첫 대기(초): 10→20→40→80→160
RESTART_BACKOFF_MAX = int(os.environ.get("RESTART_BACKOFF_MAX", "160"))         # 대기 상한(초)
# 쿼리가 이 시간(초) 이상 정상 가동했으면 연속 실패 카운터를 리셋(며칠에 걸친 산발적 hiccup 누적 방지).
HEALTHY_RESET_SECONDS = int(os.environ.get("HEALTHY_RESET_SECONDS", "600"))
# 재시작 N회를 다 소진했을 때 알림 보낼 Slack Incoming Webhook URL. 미설정이면 알림 skip.
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
# 알림 메시지에 표기할 배포 환경(dev/staging/prod 등).
APP_ENV = os.environ.get("APP_ENV", "dev")
SERVICE_NAME = "bronze-stream (Kafka→Iceberg Bronze)"
KST = timezone(timedelta(hours=9))

# 체크포인트는 durable·공유 가능한 스토리지여야 한다 (장애 복구 시 offset 기준).
# Iceberg 데이터는 S3FileIO(s3://)로 쓰지만, Spark 스트리밍 체크포인트는 Hadoop
# FileSystem을 쓰므로 s3a:// 스킴 + hadoop-aws가 필요하다.
CHECKPOINT = os.environ.get("CHECKPOINT_LOCATION", f"s3a://{S3_BUCKET}/checkpoints/bronze")
DB = "bronze"

TOPICS = ["ad-requests", "ad-impressions", "ad-clicks", "ad-conversions"]

# 토픽명 → Bronze 테이블명 매핑하는 딕셔너리 (하이픈 → 언더스코어)
TOPIC_TO_TABLE = {
    "ad-requests": "ad_requests",
    "ad-impressions": "ad_impressions",
    "ad-clicks": "ad_clicks",
    "ad-conversions": "ad_conversions",
}


# build_spark는 spark_common으로 이동. task.maxFailures(배치 task 자동 재시도, 기본 4)는
# 잡별 설정이라 extra_conf로 주입한다 — main()의 "쿼리 재시작 루프"보다 한 층 아래 재시도.


def ensure_tables(spark: SparkSession) -> None:
    """Bronze DB + 4개 테이블을 IF NOT EXISTS로 생성.

    4개 테이블 모두 동일 스키마. value는 raw JSON 문자열로 보존.
    dt/hour로 파티셔닝하여 Silver/Gold의 시간 윈도우 처리를 빠르게 한다.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DB}")

    for table in TOPIC_TO_TABLE.values():
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{DB}.{table} (
                key             string,
                value           string,
                topic           string,
                kafka_partition int,
                kafka_offset    bigint,
                kafka_timestamp timestamp,
                ingested_at     timestamp,
                dt              string,
                hour            int
            )
            USING iceberg
            PARTITIONED BY (dt, hour)
            TBLPROPERTIES (
                'format-version' = '2',
                'write.target-file-size-bytes' = '134217728'
            )
            """
        )
    print(f"[INFO] Bronze 테이블 준비 완료: {list(TOPIC_TO_TABLE.values())}")


def notify_slack(payload: dict) -> None:
    """Slack Incoming Webhook으로 payload(dict)를 POST한다.

    - SLACK_WEBHOOK_URL 미설정이면 조용히 skip한다(로컬 개발 땐 무음).
    - 알림 전송이 실패해도 적재 잡을 죽이지 않는다(로그만 남김).
    - 추가 패키지 없이 표준 라이브러리 urllib만 사용한다.
    """
    if not SLACK_WEBHOOK_URL:
        return
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        print("[INFO] Slack 알림 전송 완료")
    except Exception as e:  # 알림 실패가 적재 잡을 죽이면 안 됨
        print(f"[WARN] Slack 알림 전송 실패: {e}")


def build_failure_alert(attempts_used: int, error_line: str) -> dict:
    """재시작 소진 시 보낼 운영용 Slack 경보 payload를 만든다(컬러 스트라이프 + 필드).

    심각도/서비스/환경/호스트/시각/영향/조치를 담아 on-call이 바로 판단·대응하게 한다.
    """
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S KST")
    host = socket.gethostname()
    err = (error_line or "").strip() or "(빈 오류 메시지)"
    if len(err) > 800:                       # Slack 블록 길이 보호
        err = err[:800] + " …(생략)"
    return {
        # 알림 배너/검색/폴백용 요약 (블록 미지원 클라이언트도 이 줄은 보임)
        # <!here> = @here 태깅(현재 활성 멤버 호출). 긴급 경보라 사람을 부른다.
        "text": f"<!here> 🚨 [CRITICAL] Bronze 적재 실패 — 재시작 {attempts_used}회 모두 실패 ({APP_ENV}) {ts}",
        "attachments": [
            {
                "color": "#D7263D",          # 위험(빨강) 스트라이프
                "blocks": [
                    {
                        "type": "header",
                        "text": {"type": "plain_text", "text": "🚨 Bronze 적재 실패 (CRITICAL)", "emoji": True},
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*서비스*\n{SERVICE_NAME}"},
                            {"type": "mrkdwn", "text": f"*환경*\n`{APP_ENV}`"},
                            {"type": "mrkdwn", "text": f"*상태*\n재시작 {attempts_used}회 모두 실패 → 잡 종료"},
                            {"type": "mrkdwn", "text": f"*호스트*\n`{host}`"},
                            {"type": "mrkdwn", "text": f"*발생시각*\n{ts}"},
                            {"type": "mrkdwn", "text": f"*체크포인트*\n`{CHECKPOINT}`"},
                        ],
                    },
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": f"*마지막 오류*\n```{err}```"},
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                "*영향*\nKafka→Bronze 적재 중단. 다운스트림 Silver/Gold 신선도 지연 가능. "
                                "(컨테이너는 자동 재기동 시도 중)"
                            ),
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                "*조치*\n"
                                "• `docker logs spark-bronze` 로 마지막 스택트레이스 확인\n"
                                "• Kafka 브로커 연결/토픽 상태 점검\n"
                                "• S3 권한·용량, AWS 자격증명 만료 여부 점검"
                            ),
                        },
                    },
                    {
                        "type": "context",
                        "elements": [
                            {"type": "mrkdwn", "text": f"ad-event-lakehouse · `bronze_stream.py` · checkpoint 기준 재처리됨(at-least-once)"}
                        ],
                    },
                ],
            }
        ],
    }


def write_batch(batch_df, epoch_id: int) -> None:
    """각 마이크로배치를 토픽별로 분리해 해당 Bronze 테이블에 append.

    하나의 스트림이 4개 토픽을 함께 읽으므로, 배치 안에서 topic 컬럼으로
    필터링하여 4개 테이블로 라우팅한다.
    """
    # 메모리 부족 시 디스크로 흘려 OOM 방지 (4개 토픽 필터/쓰기에서 재사용)
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        total = batch_df.count()
        if total == 0:
            return
        for topic, table in TOPIC_TO_TABLE.items():
            # topic 컬럼은 테이블 스키마에 포함되므로 그대로 둔다.
            part = batch_df.filter(col("topic") == topic)
            part.writeTo(f"{CATALOG}.{DB}.{table}").append()
        print(f"[INFO] epoch {epoch_id}: {total}건 적재 완료")
    finally:
        batch_df.unpersist()


def main() -> None:
    spark = build_spark("bronze-stream", {"spark.task.maxFailures": "4"})
    spark.sparkContext.setLogLevel("WARN")

    ensure_tables(spark)

    print(f"[INFO] Kafka 구독: {KAFKA_BOOTSTRAP} | topics={TOPICS}")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", ",".join(TOPICS))
        .option("startingOffsets", "earliest")
        # 배치당 읽는 메시지 수 제한 — 백로그를 한 번에 읽다 OOM 나는 것 방지.
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    # raw payload 보존 + Kafka 메타 + 파티션 컬럼 파생
    bronze = raw.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingested_at"),
        date_format(col("timestamp"), "yyyy-MM-dd").alias("dt"),
        hour(col("timestamp")).alias("hour"),
    )

    # ── 재시작 루프 ("실패하면 다시 켜주는 코드") ──────────────────────────────
    # 스트리밍 쿼리가 죽으면 컨테이너를 통째로 재기동(느림)하기 전에, 잡 안에서 체크포인트
    # 기준으로 다시 시작한다. 일시 오류(S3 throttle 등)는 backoff 후 빠르게 회복하고,
    # 지속 오류는 5회까지 시도하다 소진되면 Slack으로 1번 알리고 종료한다(→ Docker가 최후의 그물).
    # 실패한 배치의 offset은 커밋되지 않았으므로, 재시작 시 그 배치를 다시 읽어 재처리한다
    # (중복이 생겨도 Silver가 event_id 기준 dedup으로 흡수).
    attempts = 0
    while True:
        started_at = time.monotonic()
        query = (
            bronze.writeStream.foreachBatch(write_batch)
            .option("checkpointLocation", CHECKPOINT)
            .trigger(processingTime="60 seconds")
            .start()
        )
        print(f"[INFO] Bronze 스트리밍 시작 | checkpoint={CHECKPOINT} | 재시작={attempts}/{MAX_STREAM_RESTARTS}")
        try:
            query.awaitTermination()
            break  # 쿼리가 정상 종료(드묾) → 루프 탈출
        except StreamingQueryException as e:
            uptime = time.monotonic() - started_at
            # 충분히 오래 정상 가동했으면 연속 실패 카운터 리셋 (산발적 hiccup이 누적 소진되지 않게).
            if uptime >= HEALTHY_RESET_SECONDS:
                print(f"[INFO] 직전 가동 {uptime:.0f}s(>= {HEALTHY_RESET_SECONDS}s) → 재시작 카운터 리셋")
                attempts = 0
            attempts += 1

            first_line = str(e).splitlines()[0] if str(e).strip() else type(e).__name__
            if attempts > MAX_STREAM_RESTARTS:
                # 재시작 N회를 모두 소진 → 운영용 Slack 경보 1번 후 비정상 종료(Docker가 컨테이너 재기동).
                print(f"[ERROR] 재시작 {MAX_STREAM_RESTARTS}회 소진 — 종료. {first_line}")
                notify_slack(build_failure_alert(MAX_STREAM_RESTARTS, first_line))
                raise  # 비정상 종료 → docker restart: on-failure

            backoff = min(RESTART_BACKOFF_SECONDS * (2 ** (attempts - 1)), RESTART_BACKOFF_MAX)
            print(
                f"[WARN] 스트리밍 실패({first_line}) — {backoff}s 후 재시작 "
                f"({attempts}/{MAX_STREAM_RESTARTS})"
            )
            time.sleep(backoff)


if __name__ == "__main__":
    main()
