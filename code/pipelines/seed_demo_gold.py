"""
seed_demo_gold.py — 데일리 리포트 데모용 합성 백필 (일회성)

gold.campaign_daily_stats에 최근 N일(기본 35일=5주)의 일별 KPI를 합성 적재한다. 요일 계절성
(주중↑/주말↓) + 노이즈 + 약한 추세를 넣어 WoW(D-7)·MoM(D-28) 비교와 추세 차트가 의미있게 보이게 한다.

- 합성 row는 campaign_id >= SYNTHETIC_BASE(9000_0000)로 두어 실제 캠페인(1000_xxxx)과 안 섞임.
- 멱등: 적재 전 기존 합성 row를 DELETE 후 append (재실행해도 중복 없음).
- 건강한 값: requests≥impressions≥clicks≥conversions(퍼널 단조), 비율 ∈ [0,1] → pipeline_health FAIL 안 깸.
  (cost 정합성 체크는 합성 gold라 WARN — FAIL 아님.)
- 제거: --clean 으로 합성 row만 삭제해 원복.

실행:
  spark-submit seed_demo_gold.py            # 35일 합성 적재
  spark-submit seed_demo_gold.py --days 42  # 6주
  spark-submit seed_demo_gold.py --clean    # 합성 데이터 제거
  (필요 환경변수: S3_BUCKET, AWS_REGION)
"""

import argparse
import random
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import types as T

from spark_common import CATALOG, build_spark

TARGET = f"{CATALOG}.gold.campaign_daily_stats"
SYNTHETIC_BASE = 90000000          # 합성 campaign_id 시작 (실제와 분리)
N_CAMPAIGNS = 5

SCHEMA = T.StructType([
    T.StructField("campaign_id", T.IntegerType()),
    T.StructField("event_date", T.DateType()),
    T.StructField("requests", T.LongType()),
    T.StructField("impressions", T.LongType()),
    T.StructField("clicks", T.LongType()),
    T.StructField("conversions", T.LongType()),
    T.StructField("fill_rate", T.DoubleType()),
    T.StructField("ctr", T.DoubleType()),
    T.StructField("cvr", T.DoubleType()),
    T.StructField("cost", T.DoubleType()),
    T.StructField("cpa", T.DoubleType()),
    T.StructField("roas", T.DoubleType()),
    T.StructField("updated_at", T.TimestampType()),
])


def make_rows(days: int) -> list:
    """최근 days일 × N_CAMPAIGNS의 건강한 합성 KPI row 생성."""
    rows = []
    today = date.today()
    now = datetime.now(timezone.utc)
    campaigns = [SYNTHETIC_BASE + 1 + i for i in range(N_CAMPAIGNS)]
    for d in range(days):
        ev = today - timedelta(days=d)
        weekday_factor = 1.0 if ev.weekday() < 5 else 0.6      # 주말은 트래픽 60%
        trend = 1.0 + (days - d) * 0.004                        # 최근일수록 소폭 상승
        for c in campaigns:
            requests = int(random.randint(40000, 80000) * weekday_factor * trend * random.uniform(0.9, 1.1))
            impressions = int(requests * random.uniform(0.75, 0.85))      # fill ~80%
            clicks = int(impressions * random.uniform(0.020, 0.030))      # ctr ~2.5%
            conversions = int(clicks * random.uniform(0.030, 0.040))      # cvr ~3.5%
            cost = round(clicks * random.uniform(0.3, 0.8), 2)            # CPC
            # 비율·파생값은 정수 카운트에서 재계산 → 완전 일관(퍼널 단조·범위 보장)
            fill_rate = round(impressions / requests, 4) if requests else 0.0
            ctr = round(clicks / impressions, 4) if impressions else 0.0
            cvr = round(conversions / clicks, 4) if clicks else 0.0
            cpa = round(cost / conversions, 2) if conversions else 0.0
            roas = round(conversions * 10.0 / cost, 2) if cost else 0.0
            rows.append((c, ev, requests, impressions, clicks, conversions,
                         fill_rate, ctr, cvr, cost, cpa, roas, now))
    return rows


def run(args) -> None:
    spark = build_spark("seed-demo-gold")
    spark.sparkContext.setLogLevel("WARN")

    # 멱등/clean 공통: 기존 합성 row 제거
    spark.sql(f"DELETE FROM {TARGET} WHERE campaign_id >= {SYNTHETIC_BASE}")
    if args.clean:
        print(f"[INFO] 합성 데이터 제거 완료 (campaign_id >= {SYNTHETIC_BASE})")
        return

    rows = make_rows(args.days)
    spark.createDataFrame(rows, schema=SCHEMA).writeTo(TARGET).append()
    print(f"[INFO] 합성 백필 완료 | {args.days}일 × {N_CAMPAIGNS}캠페인 = {len(rows)}행 → {TARGET}")
    spark.sql(
        f"SELECT event_date, count(*) AS campaigns, sum(impressions) AS impr, sum(clicks) AS clk "
        f"FROM {TARGET} WHERE campaign_id >= {SYNTHETIC_BASE} GROUP BY event_date ORDER BY event_date DESC"
    ).show(10, truncate=False)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=35, help="합성할 일수(과거로). 기본 35(5주).")
    p.add_argument("--clean", action="store_true", help="합성 데이터만 삭제하고 종료.")
    run(p.parse_args())
