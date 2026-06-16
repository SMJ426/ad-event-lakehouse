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

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, hour
from pyspark.storagelevel import StorageLevel

# ── 환경 설정 ────────────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
# 배치당 읽는 메시지 수. 소비속도 = 이 값 / trigger(60s). 인프라단에서 조절.
MAX_OFFSETS_PER_TRIGGER = os.environ.get("MAX_OFFSETS_PER_TRIGGER", "20000")

WAREHOUSE = f"s3://{S3_BUCKET}/warehouse"
# 체크포인트는 durable·공유 가능한 스토리지여야 한다 (장애 복구 시 offset 기준).
# Iceberg 데이터는 S3FileIO(s3://)로 쓰지만, Spark 스트리밍 체크포인트는 Hadoop
# FileSystem을 쓰므로 s3a:// 스킴 + hadoop-aws가 필요하다.
CHECKPOINT = os.environ.get("CHECKPOINT_LOCATION", f"s3a://{S3_BUCKET}/checkpoints/bronze")
CATALOG = "glue"
DB = "bronze"

TOPICS = ["ad-requests", "ad-impressions", "ad-clicks", "ad-conversions"]

# 토픽명 → Bronze 테이블명 매핑하는 딕셔너리 (하이픈 → 언더스코어)
TOPIC_TO_TABLE = {
    "ad-requests": "ad_requests",
    "ad-impressions": "ad_impressions",
    "ad-clicks": "ad_clicks",
    "ad-conversions": "ad_conversions",
}


def build_spark() -> SparkSession:
    """Iceberg Glue Catalog + S3FileIO가 설정된 SparkSession 생성."""
    return (
        SparkSession.builder.appName("bronze-stream")
        # Iceberg SQL 확장 활성화
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # 'glue' 카탈로그를 Iceberg + AWS Glue Catalog로 연결
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{CATALOG}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config("spark.sql.catalog.glue.client.region", AWS_REGION)
        # s3a (스트리밍 체크포인트용 Hadoop FileSystem) 설정
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        .getOrCreate()
    )


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
            """
        )
    print(f"[INFO] Bronze 테이블 준비 완료: {list(TOPIC_TO_TABLE.values())}")


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
    spark = build_spark()
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

    query = (
        bronze.writeStream.foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="60 seconds")
        .start()
    )

    print(f"[INFO] Bronze 스트리밍 시작 | checkpoint={CHECKPOINT}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
