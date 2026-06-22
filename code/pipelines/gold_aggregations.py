"""
gold_aggregations.py — Silver processed_events → Gold KPI 집계 배치 잡

역할:
  Silver(이벤트 단위)를 비즈니스 KPI로 집계한 서빙용 Gold 테이블 3개를 만든다.
  대시보드(소비자)의 데이터 계약에 맞춰 설계:
    - campaign_daily_stats : 캠페인 성과(CTR/CVR/CPA/ROAS, 일별 추세)
    - banner_daily_stats   : 소재(배너) 성과(CTR, peak_hour)
    - hourly_funnel        : 시간대 퍼널/분포(24h fill→CTR→CVR)

증분 처리 (전량 재계산 아님):
  실제 파이프라인은 날짜별로 데이터가 누적된다. event_date(이벤트 발생시각)가 아니라
  updated_at(Silver 처리시각)으로 "최근 변경분"을 잡고, 그 distinct event_date 파티션만
  재집계해 overwritePartitions 한다. → criteo 과거날짜(2024)도 방금 처리됐으면 잡히고,
  late data 자동 반영, 매일 바뀐 날짜만 갱신. (최초 1회만 --all로 전량 백필)

cost 정의:
  criteo는 click+conversion, dummy는 전 이벤트가 cost를 가져 단순 SUM 시 중복/폭증.
  → 광고비 = SUM(cost) FILTER(event_type='click') (CPC 모델로 통일).

실행:
  spark-submit gold_aggregations.py --all                # 최초 전량 백필
  spark-submit gold_aggregations.py --lookback-days 3    # 증분 (기본)
  (필요 환경변수: S3_BUCKET, AWS_REGION)
"""

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── 환경 설정 ────────────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
WAREHOUSE = f"s3://{S3_BUCKET}/warehouse"
CATALOG = "glue"

SILVER = f"{CATALOG}.silver.processed_events"
CAMPAIGN_DAILY = f"{CATALOG}.gold.campaign_daily_stats"
BANNER_DAILY = f"{CATALOG}.gold.banner_daily_stats"
HOURLY_FUNNEL = f"{CATALOG}.gold.hourly_funnel"

# ROAS용 가정 단가 — 매출 데이터가 없으므로 전환당 매출을 상수로 가정한다(가정임을 명시).
REVENUE_PER_CONVERSION = float(os.environ.get("GOLD_REVENUE_PER_CONVERSION", "10.0"))


# ── Spark ────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    """Iceberg Glue Catalog + S3FileIO SparkSession (silver_processed와 동일 패턴)."""
    return (
        SparkSession.builder.appName("gold-aggregations")
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


def ensure_tables(spark: SparkSession) -> None:
    """gold DB + 3개 KPI 테이블을 IF NOT EXISTS로 생성. (DDL은 gold_tables.sql)"""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.gold")
    props = (
        "USING iceberg PARTITIONED BY (event_date) "
        "TBLPROPERTIES ('format-version'='2','write.target-file-size-bytes'='134217728')"
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {CAMPAIGN_DAILY} (
            campaign_id int, event_date date,
            requests bigint, impressions bigint, clicks bigint, conversions bigint,
            fill_rate double, ctr double, cvr double,
            cost double, cpa double, roas double,
            updated_at timestamp
        ) {props}"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {BANNER_DAILY} (
            banner_id string, event_date date,
            impressions bigint, clicks bigint, ctr double,
            peak_hour int, cost double,
            updated_at timestamp
        ) {props}"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {HOURLY_FUNNEL} (
            event_date date, hour int,
            requests bigint, impressions bigint, clicks bigint, conversions bigint,
            fill_rate double, ctr double, cvr double,
            updated_at timestamp
        ) {props}"""
    )


# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _cnt(event_type: str):
    """event_type별 이벤트 수 (이벤트 단위 Silver → 조건부 합)."""
    return F.sum(F.when(F.col("event_type") == event_type, 1).otherwise(0))


def _click_cost():
    """광고비 = click 이벤트의 cost만 합산 (CPC 모델, 중복 합산 방지)."""
    return F.sum(F.when(F.col("event_type") == "click", F.col("cost")).otherwise(0.0))


def _ratio(num: str, den: str):
    """num/den. den=0이면 NULL (0나눗셈 방어)."""
    return F.round(F.col(num) / F.when(F.col(den) == 0, None).otherwise(F.col(den)), 6)


# ── 증분 대상 파티션 선정 ─────────────────────────────────────────────────────

def affected_event_dates(spark: SparkSession, lookback_days: int) -> list:
    """updated_at(Silver 처리시각)이 최근 lookback_days 이내인 행의 distinct event_date.

    event_date(이벤트 발생시각)가 아니라 처리시각으로 잡아야 criteo 과거날짜도 포함된다.
    """
    rows = spark.sql(
        f"SELECT DISTINCT event_date FROM {SILVER} "
        f"WHERE updated_at >= current_timestamp() - INTERVAL {lookback_days} DAYS"
    ).collect()
    return [r["event_date"] for r in rows]


# ── 집계 ─────────────────────────────────────────────────────────────────────

def agg_campaign_daily(silver: DataFrame) -> DataFrame:
    base = silver.groupBy("campaign_id", "event_date").agg(
        _cnt("request").alias("requests"),
        _cnt("impression").alias("impressions"),
        _cnt("click").alias("clicks"),
        _cnt("conversion").alias("conversions"),
        F.round(_click_cost(), 6).alias("cost"),
    )
    return (
        base.withColumn("fill_rate", _ratio("impressions", "requests"))
        .withColumn("ctr", _ratio("clicks", "impressions"))
        .withColumn("cvr", _ratio("conversions", "clicks"))
        .withColumn("cpa", _ratio("cost", "conversions"))
        .withColumn(
            "roas",
            F.round(
                (F.col("conversions") * F.lit(REVENUE_PER_CONVERSION))
                / F.when(F.col("cost") == 0, None).otherwise(F.col("cost")),
                6,
            ),
        )
        .withColumn("updated_at", F.current_timestamp())
        .select(
            "campaign_id", "event_date", "requests", "impressions", "clicks",
            "conversions", "fill_rate", "ctr", "cvr", "cost", "cpa", "roas", "updated_at",
        )
    )


def agg_banner_daily(silver: DataFrame) -> DataFrame:
    base = silver.groupBy("banner_id", "event_date").agg(
        _cnt("impression").alias("impressions"),
        _cnt("click").alias("clicks"),
        F.round(_click_cost(), 6).alias("cost"),
    ).withColumn("ctr", _ratio("clicks", "impressions"))

    # peak_hour = 해당 banner/일에서 impression이 가장 많은 시(hour)
    hourly_imp = (
        silver.where(F.col("event_type") == "impression")
        .groupBy("banner_id", "event_date", F.hour("event_timestamp").alias("hour"))
        .count()
    )
    w = Window.partitionBy("banner_id", "event_date").orderBy(F.col("count").desc(), F.col("hour"))
    peak = (
        hourly_imp.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .select("banner_id", "event_date", F.col("hour").alias("peak_hour"))
    )
    return (
        base.join(peak, ["banner_id", "event_date"], "left")
        .withColumn("updated_at", F.current_timestamp())
        .select("banner_id", "event_date", "impressions", "clicks", "ctr",
                "peak_hour", "cost", "updated_at")
    )


def agg_hourly_funnel(silver: DataFrame) -> DataFrame:
    base = (
        silver.groupBy("event_date", F.hour("event_timestamp").alias("hour"))
        .agg(
            _cnt("request").alias("requests"),
            _cnt("impression").alias("impressions"),
            _cnt("click").alias("clicks"),
            _cnt("conversion").alias("conversions"),
        )
    )
    return (
        base.withColumn("fill_rate", _ratio("impressions", "requests"))
        .withColumn("ctr", _ratio("clicks", "impressions"))
        .withColumn("cvr", _ratio("conversions", "clicks"))
        .withColumn("updated_at", F.current_timestamp())
        .select("event_date", "hour", "requests", "impressions", "clicks",
                "conversions", "fill_rate", "ctr", "cvr", "updated_at")
    )


# ── 메인 ─────────────────────────────────────────────────────────────────────

def run(args) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)

    if args.all:
        dates = None
        print("[INFO] Gold 집계 | 전량(--all) 백필")
    else:
        dates = affected_event_dates(spark, args.lookback_days)
        if not dates:
            print(f"[INFO] 최근 {args.lookback_days}일 내 변경된 파티션 없음 — 종료.")
            return
        print(f"[INFO] Gold 집계 | 증분 | 재계산 event_date: {sorted(map(str, dates))}")

    silver = spark.table(SILVER)
    if dates is not None:
        silver = silver.where(F.col("event_date").isin(dates))

    print(f"[INFO] REVENUE_PER_CONVERSION(ROAS 가정 단가) = {REVENUE_PER_CONVERSION}")

    # 집계 → 해당 event_date 파티션만 overwrite (증분·멱등)
    agg_campaign_daily(silver).writeTo(CAMPAIGN_DAILY).overwritePartitions()
    print("[INFO] campaign_daily_stats overwrite 완료")
    agg_banner_daily(silver).writeTo(BANNER_DAILY).overwritePartitions()
    print("[INFO] banner_daily_stats overwrite 완료")
    agg_hourly_funnel(silver).writeTo(HOURLY_FUNNEL).overwritePartitions()
    print("[INFO] hourly_funnel overwrite 완료")

    # 요약 (검증용)
    for name, tbl in [("campaign_daily_stats", CAMPAIGN_DAILY),
                      ("banner_daily_stats", BANNER_DAILY),
                      ("hourly_funnel", HOURLY_FUNNEL)]:
        n = spark.table(tbl).count()
        print(f"[SUMMARY] {name}: {n} rows")
    print("[INFO] Gold 집계 완료.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--all", action="store_true",
                   help="전량 백필(최초 1회). 미지정 시 updated_at 기반 증분.")
    p.add_argument("--lookback-days", type=int, default=3,
                   help="증분: updated_at이 최근 N일 내인 파티션만 재계산. 기본 3.")
    p.add_argument("--revenue-per-conversion", type=float, default=None,
                   help="ROAS 가정 단가 오버라이드(기본 env GOLD_REVENUE_PER_CONVERSION 또는 10.0).")
    args = p.parse_args()
    if args.revenue_per_conversion is not None:
        REVENUE_PER_CONVERSION = args.revenue_per_conversion
    run(args)
