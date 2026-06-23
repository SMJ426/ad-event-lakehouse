"""
silver_processed.py — Bronze raw → Silver processed_events 정제 배치 잡

역할:
  Bronze Iceberg(raw JSON 보존)를 읽어 파싱·통일·중복제거하고,
  이벤트 단위(event_id 1개 = 1 row) processed_events 테이블에 MERGE INTO로 증분 반영한다.

처리 흐름:
  1. sliding window — glue.bronze.ad_* 4개 테이블에서 최근 N일치(dt 기준)만 읽음
  2. source 분기 파싱 — value(JSON)를 dummy(AdEvent)/criteo(CriteoRawEvent)별로 파싱·통일
  3. dedup — event_id 기준 최신 1건 (ROW_NUMBER, 재처리 중복 방어)
  4. conversion_delay_sec — conversion 이벤트를 click에 auction_id로 join (1:1)
  5. MERGE INTO — event_id 기준 upsert (멱등 재실행 + 지연 전환 반영, COW)

왜 이벤트 단위인가:
  criteo는 click 1건당 impression 40개를 같은 auction_id로 합성한다. 강의식
  funnel-join(auction_id로 collapse)은 40배 폭증하므로, 이벤트 단위로 통일한다.

실행:
  spark-submit silver_processed.py --window-days 7
  (필요 환경변수: S3_BUCKET, AWS_REGION)
"""

import argparse
import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── 환경 설정 ────────────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
WAREHOUSE = f"s3://{S3_BUCKET}/warehouse"
CATALOG = "glue"

BRONZE_TABLES = ["ad_requests", "ad_impressions", "ad_clicks", "ad_conversions"]
TARGET = f"{CATALOG}.silver.processed_events"

# criteo timestamp는 수집 시작 기준 '상대 초'다. 절대 시각으로 변환할 기준점(BASE).
# 시뮬레이션 가정 상수 — 2024-01-01 00:00:00 UTC. event_date 파티션이 이 값에 의존.
CRITEO_BASE_TS = 1_704_067_200  # 2024-01-01T00:00:00Z epoch seconds

# dummy_producer의 IAB 카테고리 풀과 동일. criteo cat1 → 이 배열에서 (cat1 % N) 선택.
SITE_CATS = ["IAB1", "IAB2", "IAB3", "IAB4", "IAB5", "IAB7", "IAB9", "IAB13"]

# 최종 processed_events 컬럼 순서 (MERGE source/target 일치용)
FINAL_COLS = [
    "event_id", "event_type", "source", "auction_id", "campaign_id",
    "event_timestamp", "event_date", "uid", "banner_id", "site_cat",
    "device_type", "os", "country", "cost", "conversion",
    "conversion_delay_sec", "updated_at",
]

# 허용 event_type 화이트리스트 (validate용)
VALID_EVENT_TYPES = ["request", "impression", "click", "conversion"]

# ── source별 JSON 스키마 ─────────────────────────────────────────────────────
DUMMY_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("source", StringType()),
    StructField("auction_id", StringType()),
    StructField("campaign_id", IntegerType()),
    StructField("banner_id", StringType()),
    StructField("banner_w", IntegerType()),
    StructField("banner_h", IntegerType()),
    StructField("banner_pos", IntegerType()),
    StructField("site_domain", StringType()),
    StructField("site_cat", StringType()),
    StructField("device_type", IntegerType()),
    StructField("os", StringType()),
    StructField("country", StringType()),
    StructField("uid", StringType()),
    StructField("floor_price", DoubleType()),
    StructField("bid_price", DoubleType()),
    StructField("timestamp", LongType()),
    StructField("produced_at", StringType()),
])

CRITEO_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("source", StringType()),
    StructField("auction_id", StringType()),
    StructField("produced_at", StringType()),
    StructField("campaign", IntegerType()),
    StructField("uid", StringType()),
    StructField("cost", DoubleType()),
    StructField("timestamp", LongType()),
    StructField("conversion", IntegerType()),
    StructField("cat1", LongType()),
    StructField("cat2", LongType()),
    StructField("cat3", LongType()),
    StructField("cat4", LongType()),
    StructField("cat5", LongType()),
    StructField("cat6", LongType()),
    StructField("cat7", LongType()),
    StructField("cat8", LongType()),
    StructField("cat9", LongType()),
])


# ── Spark ────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    """Iceberg Glue Catalog + S3FileIO가 설정된 SparkSession 생성. (bronze_stream과 동일 패턴)"""
    return (
        SparkSession.builder.appName("silver-processed")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
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
        .config(f"spark.sql.catalog.{CATALOG}.client.region", AWS_REGION)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
        .getOrCreate()
    )


def ensure_table(spark: SparkSession) -> None:
    """Silver DB + processed_events 테이블을 IF NOT EXISTS로 생성. (DDL은 silver_tables.sql)"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.silver")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET} (
            event_id              string,
            event_type            string,
            source                string,
            auction_id            string,
            campaign_id           int,
            event_timestamp       timestamp,
            event_date            date,
            uid                   string,
            banner_id             string,
            site_cat              string,
            device_type           int,
            os                    string,
            country               string,
            cost                  double,
            conversion            int,
            conversion_delay_sec  bigint,
            updated_at            timestamp
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode' = 'copy-on-write',
            'write.merge.mode'  = 'copy-on-write',
            'write.delete.mode' = 'copy-on-write',
            'write.target-file-size-bytes' = '134217728'
        )
        """
    )


# ── 1. sliding window 읽기 ───────────────────────────────────────────────────

def read_bronze_window(
    spark: SparkSession,
    window_days: int,
    hour: int | None = None,
    lookback_hours: int | None = None,
) -> DataFrame:
    """Bronze 4개 테이블에서 대상 구간만 읽어 union.

    각 테이블 스키마 동일: key, value, topic, kafka_partition, kafka_offset,
    kafka_timestamp, ingested_at, dt, hour. 필요한 value/ingested_at만 사용.

    두 읽기 모드:
      - sliding(기본): dt >= current_date - window_days (일배치용).
      - incremental: lookback_hours 지정 시 ingested_at >= now - N시간 (15분 등 잦은 배치용).
        Bronze 적재시각 기준이라 늦게 도착한 데이터도 도착 시점에 잡힘. 매 실행 전체 재독 방지.

    hour 지정 시 해당 시(hour) 파티션만 읽는다 — 검증/개발용 슬라이스 축소.
    """
    parts = []
    for t in BRONZE_TABLES:
        df = spark.table(f"{CATALOG}.bronze.{t}")
        if lookback_hours is not None:
            df = df.where(
                F.col("ingested_at") >= F.expr(f"current_timestamp() - INTERVAL {int(lookback_hours)} HOURS")
            )
        else:
            df = df.where(
                F.col("dt") >= F.date_format(F.date_sub(F.current_date(), window_days), "yyyy-MM-dd")
            )
        if hour is not None and hour >= 0:   # -1 또는 None = 전체 hour
            df = df.where(F.col("hour") == hour)
        parts.append(df.select("value", "ingested_at"))
    return reduce(DataFrame.unionByName, parts)


# ── 2. source 분기 파싱 + 통일 ───────────────────────────────────────────────

def parse_dummy(raw: DataFrame) -> DataFrame:
    """source=dummy(AdEvent) 파싱. 이미 OpenRTB 형식이라 거의 직매핑."""
    d = (
        raw.where(F.get_json_object("value", "$.source") == "dummy")
        .select(F.from_json("value", DUMMY_SCHEMA).alias("e"), "ingested_at")
        .select("e.*", "ingested_at")
    )
    ts = F.timestamp_seconds("timestamp")
    return d.select(
        "event_id", "event_type", "source", "auction_id",
        F.col("campaign_id").cast("int").alias("campaign_id"),
        ts.alias("event_timestamp"),
        F.to_date(ts).alias("event_date"),
        "uid", "banner_id", "site_cat",
        F.col("device_type").cast("int").alias("device_type"),
        "os", "country",
        F.col("bid_price").cast("double").alias("cost"),
        F.when(F.col("event_type") == "conversion", F.lit(1)).otherwise(F.lit(0)).alias("conversion"),
        "ingested_at",
    )


def parse_criteo(raw: DataFrame) -> DataFrame:
    """source=criteo(CriteoRawEvent) 파싱 + OpenRTB 통일 변환."""
    c = (
        raw.where(F.get_json_object("value", "$.source") == "criteo")
        .select(F.from_json("value", CRITEO_SCHEMA).alias("e"), "ingested_at")
        .select("e.*", "ingested_at")
    )
    ts = F.timestamp_seconds(F.lit(CRITEO_BASE_TS) + F.col("timestamp"))
    iab = F.element_at(F.array(*[F.lit(x) for x in SITE_CATS]),
                       (F.col("cat1") % len(SITE_CATS) + 1).cast("int"))
    return c.select(
        "event_id", "event_type", "source", "auction_id",
        F.col("campaign").cast("int").alias("campaign_id"),       # campaign → campaign_id
        ts.alias("event_timestamp"),                              # BASE + 상대초 → 절대시각
        F.to_date(ts).alias("event_date"),
        "uid",
        F.concat_ws("_", F.col("campaign"), F.col("cat1"), F.col("cat2")).alias("banner_id"),
        iab.alias("site_cat"),                                    # cat1 → IAB 매핑
        F.lit(None).cast("int").alias("device_type"),            # criteo 없음 → NULL
        F.lit(None).cast("string").alias("os"),
        F.lit(None).cast("string").alias("country"),
        (F.col("cost") * 1000).cast("double").alias("cost"),     # CPC → CPM
        F.when(F.col("event_type") == "conversion", F.lit(1)).otherwise(F.lit(0)).alias("conversion"),
        "ingested_at",
    )


# ── 2.5 품질 검증 (validation) ────────────────────────────────────────────────

def validate(unified: DataFrame) -> tuple[DataFrame, DataFrame]:
    """품질 규칙으로 reject_reason을 매긴 뒤 valid / rejected로 분리.

    무효 행은 통과시키지 않는다(drop). 단 제거 건수·사유는 run()에서 로그로 남겨
    데이터 품질을 관측할 수 있게 한다(별도 격리 테이블은 두지 않음).
    주의: criteo는 device_type/os/country가 정상 NULL이므로, null 검사는 두 source
    공통 키(event_id / campaign_id / uid)에만 적용한다 (오격리 방지).
    when 체인은 첫 매치가 우선이다.
    """
    ts_lower = F.lit("2020-01-01 00:00:00").cast("timestamp")
    ts_upper = F.expr("current_timestamp() + INTERVAL 1 DAY")

    reason = (
        F.when(F.col("event_id").isNull(), "null_event_id")
        .when(~F.col("event_type").isin(VALID_EVENT_TYPES), "bad_event_type")
        .when(F.col("campaign_id").isNull(), "null_campaign_id")
        .when(F.col("uid").isNull(), "null_uid")
        .when(F.col("cost") < 0, "negative_cost")
        .when(
            F.col("event_timestamp").isNull()
            | (F.col("event_timestamp") < ts_lower)
            | (F.col("event_timestamp") > ts_upper),
            "timestamp_out_of_range",
        )
        .otherwise(F.lit(None).cast("string"))
    )

    tagged = unified.withColumn("reject_reason", reason)
    valid = tagged.where(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = tagged.where(F.col("reject_reason").isNotNull())
    return valid, rejected


# ── 3. dedup ─────────────────────────────────────────────────────────────────

def dedup(unified: DataFrame) -> DataFrame:
    """event_id 기준 최신 1건만 (ingested_at 최신). 재처리 중복 방어."""
    from pyspark.sql.window import Window

    w = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())
    return (
        unified.withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn", "ingested_at")
    )


# ── 4. conversion_delay_sec ──────────────────────────────────────────────────

def enrich_conversion_delay(spark: SparkSession, events: DataFrame) -> DataFrame:
    """conversion 이벤트에 conversion_delay_sec 계산.

    conversion ↔ click을 auction_id로 left-join (한 auction에 click 1·conversion 0~1 → 1:1).
    click 후보 = 이번 window의 click ∪ 기존 processed_events의 click (지연 전환 대비).
    delay = conversion.event_timestamp - click.event_timestamp (초).
    """
    window_clicks = events.where(F.col("event_type") == "click").select(
        "auction_id", F.col("event_timestamp").alias("click_ts")
    )
    existing_clicks = spark.table(TARGET).where(F.col("event_type") == "click").select(
        "auction_id", F.col("event_timestamp").alias("click_ts")
    )
    clicks = window_clicks.unionByName(existing_clicks).dropDuplicates(["auction_id"])

    conversions = events.where(F.col("event_type") == "conversion")
    non_conv = events.where(F.col("event_type") != "conversion").withColumn(
        "conversion_delay_sec", F.lit(None).cast("long")
    )

    conv_delay = (
        conversions.join(clicks, "auction_id", "left")
        .withColumn(
            "conversion_delay_sec",
            (F.col("event_timestamp").cast("long") - F.col("click_ts").cast("long")).cast("long"),
        )
        .drop("click_ts")
    )
    return non_conv.unionByName(conv_delay)


# ── 5. MERGE INTO ────────────────────────────────────────────────────────────

def merge_into(spark: SparkSession, final_df: DataFrame) -> None:
    """event_id 기준 upsert. 멱등 재실행 + 지연 전환 반영 (COW).

    조건부 UPDATE: 비즈니스 컬럼이 실제로 다를 때만 UPDATE 한다(그때만 updated_at 갱신).
    왜? sliding window가 매일 같은 행을 다시 읽어오는데, 무조건 UPDATE SET * 하면 안 바뀐
    행도 updated_at이 매일 갱신되고, Gold(updated_at 증분)가 안 바뀐 파티션도 매일 재집계한다.
    null-safe 비교(<=>)로 criteo의 정상 NULL(device/os/country)도 안전하게 다룬다.
    (README 고민 14 참고)
    """
    final_df.withColumn("updated_at", F.current_timestamp()) \
        .select(*FINAL_COLS) \
        .createOrReplaceTempView("silver_source")

    # 비교 대상 = 키(event_id)와 메타(updated_at)를 뺀 비즈니스 컬럼.
    compare_cols = [c for c in FINAL_COLS if c not in ("event_id", "updated_at")]
    unchanged = " AND ".join(f"t.{c} <=> s.{c}" for c in compare_cols)

    spark.sql(
        f"""
        MERGE INTO {TARGET} t
        USING silver_source s
        ON t.event_id = s.event_id
        WHEN MATCHED AND NOT ({unchanged}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


# ── 메인 ─────────────────────────────────────────────────────────────────────

def run(window_days: int, hour: int | None = None, lookback_hours: int | None = None) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    ensure_table(spark)
    mode = f"incremental lookback_hours={lookback_hours}" if lookback_hours is not None else f"sliding window_days={window_days}"
    print(f"[INFO] Silver 정제 시작 | mode={mode} | hour={hour} | target={TARGET}")

    raw = read_bronze_window(spark, window_days, hour, lookback_hours)
    unified = parse_dummy(raw).unionByName(parse_criteo(raw))

    # 품질 검증: 무효 행은 drop하고 valid만 다음 단계로. 제거 사유·건수는 로그로 관측.
    valid, rejected = validate(unified)
    print("[INFO] validation 완료 — drop된 행 분포(이번 run, 사유별):")
    rejected.groupBy("reject_reason").count().orderBy(F.col("count").desc()).show(20, truncate=False)

    deduped = dedup(valid)
    final_df = enrich_conversion_delay(spark, deduped)

    merge_into(spark, final_df)
    print("[INFO] MERGE 완료")

    # 간단 요약 (검증용)
    spark.sql(
        f"SELECT source, event_type, count(*) AS cnt FROM {TARGET} "
        f"GROUP BY source, event_type ORDER BY source, event_type"
    ).show(50, truncate=False)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--window-days", type=int, default=7,
                   help="Bronze에서 거슬러 읽을 일수 (sliding window). 기본 7.")
    p.add_argument("--hour", type=int, default=None,
                   help="특정 시(hour) 파티션만 처리 (검증/개발용 슬라이스 축소). 기본 전체.")
    p.add_argument("--lookback-hours", type=int, default=None,
                   help="incremental: Bronze ingested_at 기준 최근 N시간만 읽음 (잦은 배치용). "
                        "지정 시 --window-days 무시.")
    args = p.parse_args()
    run(args.window_days, args.hour, args.lookback_hours)
