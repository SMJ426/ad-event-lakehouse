"""
pipeline_health.py — 파이프라인 5분 헬스체크 (Iceberg 메타테이블 기반 자동 판정 리포트)

운영자가 한 명령으로 돌려 PASS/WARN/FAIL을 받는다. 빨간 것(⚠️/❌)만 보면 된다.

  spark-submit pipeline_health.py
  spark-submit pipeline_health.py --freshness-min 60 --min-file-mb 32 --max-snapshots 300

무엇을 보나 (Iceberg 메타테이블 $snapshots/$files + 간단 집계):
  1. 신선도   — 레이어별 최근성 (bronze=최신 스냅샷, silver/gold=updated_at)
  2. 파일     — small file (파일 수·평균 크기, 작으면 컴팩션 필요)
  3. 스냅샷   — 누적 수 (많으면 expire 필요)
  4. 처리량   — 오늘 처리된 silver 행수
  5. 중복     — silver event_id 중복 (0이어야)
  6. 퍼널정합 — gold requests≥impressions≥clicks≥conversions
  7. 비율범위 — gold fill_rate/ctr/cvr ∈ [0,1]
  8. cost정합 — gold cost == silver click cost 합

판정: ❌FAIL이 하나라도 있으면 exit 1 (Airflow/Slack 게이트로 확장 가능). ⚠️WARN은 주의.
연산·테이블 목록·SparkSession은 iceberg_maintenance / spark_common 자산을 재사용한다.
(필요 환경변수: S3_BUCKET, AWS_REGION)
"""

import argparse
import sys

from pyspark.sql import SparkSession

from spark_common import CATALOG, build_spark
from iceberg_maintenance import BRONZE_TABLES, SILVER_TABLES, GOLD_TABLES, table_metrics

# ── 판정 상태 ────────────────────────────────────────────────────────────────
OK, WARN, FAIL = "OK", "WARN", "FAIL"
ICON = {OK: "✅", WARN: "⚠️ ", FAIL: "❌"}

ALL_TABLES = BRONZE_TABLES + SILVER_TABLES + GOLD_TABLES
SILVER = SILVER_TABLES[0]                 # silver.processed_events
GOLD_CAMPAIGN = "gold.campaign_daily_stats"


def _first(spark: SparkSession, sql: str):
    return spark.sql(sql).first()


# ── 개별 체크 (각각 (status, label, detail) 리스트 반환) ──────────────────────

def check_freshness(spark: SparkSession, threshold_min: int) -> list:
    """레이어별 최근성. bronze=최신 스냅샷 committed_at, silver/gold=max(updated_at).

    임계(분) 초과면 WARN. 시각차는 Spark SQL(current_timestamp)로 계산해 tz 혼란을 피한다.
    """
    out = []
    for t in BRONZE_TABLES:
        r = _first(spark,
            f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(committed_at)))/60.0 AS m "
            f"FROM {CATALOG}.{t}.snapshots")
        out.append(_freshness_status(f"신선도 {t}", r["m"], threshold_min))
    for t in SILVER_TABLES + GOLD_TABLES:
        r = _first(spark,
            f"SELECT (unix_timestamp(current_timestamp()) - unix_timestamp(max(updated_at)))/60.0 AS m "
            f"FROM {CATALOG}.{t}")
        out.append(_freshness_status(f"신선도 {t}", r["m"], threshold_min))
    return out


def _freshness_status(label: str, lag_min, threshold_min: int):
    if lag_min is None:
        return (WARN, label, "데이터/스냅샷 없음")
    status = OK if lag_min <= threshold_min else WARN
    return (status, label, f"{lag_min:.0f}분 전 (임계 {threshold_min}분)")


def check_files_and_snapshots(spark: SparkSession, min_file_mb: float, max_snapshots: int) -> list:
    """small file(평균 크기)와 스냅샷 누적 — iceberg_maintenance.table_metrics 재사용."""
    files_out, snap_out = [], []
    for t in ALL_TABLES:
        m = table_metrics(spark, t)
        # 파일이 여러 개인데 평균이 target보다 한참 작으면 컴팩션 필요
        if m["files"] > 5 and m["avg_mb"] < min_file_mb:
            files_out.append((WARN, f"파일 {t}", f"{m['files']}개·평균 {m['avg_mb']}MB (<{min_file_mb}MB → 컴팩션)"))
        else:
            files_out.append((OK, f"파일 {t}", f"{m['files']}개·평균 {m['avg_mb']}MB"))
        status = WARN if m["snapshots"] > max_snapshots else OK
        snap_out.append((status, f"스냅샷 {t}", f"{m['snapshots']}개 (임계 {max_snapshots} → expire)"))
    return files_out + snap_out


def check_volume(spark: SparkSession) -> list:
    """오늘(updated_at 기준) 처리된 silver 행수 — 0이면 오늘 파이프라인 미실행 의심(WARN)."""
    r = _first(spark,
        f"SELECT count(*) AS total, count_if(updated_at >= current_date()) AS today "
        f"FROM {CATALOG}.{SILVER}")
    today = r["today"] or 0
    status = WARN if today == 0 else OK
    return [(status, "처리량 (silver)", f"전체 {r['total']:,}행 · 오늘 처리 {today:,}행")]


def check_duplicates(spark: SparkSession) -> list:
    r = _first(spark, f"SELECT count(*) - count(DISTINCT event_id) AS dup FROM {CATALOG}.{SILVER}")
    dup = r["dup"] or 0
    status = FAIL if dup > 0 else OK
    return [(status, "중복 event_id (silver)", f"{dup}건 (0이어야 정상)")]


def check_funnel_integrity(spark: SparkSession) -> list:
    """gold 퍼널 단조성 위반(requests≥impressions≥clicks≥conversions). >0이면 FAIL."""
    r = _first(spark,
        f"SELECT count(*) AS bad FROM {CATALOG}.{GOLD_CAMPAIGN} "
        f"WHERE impressions > requests OR clicks > impressions OR conversions > clicks")
    bad = r["bad"] or 0
    status = FAIL if bad > 0 else OK
    return [(status, "퍼널 정합성 (gold)", f"위반 {bad}행 (0이어야 정상)")]


def check_ratio_range(spark: SparkSession) -> list:
    """gold 비율(fill_rate/ctr/cvr)이 [0,1] 범위인지. 이탈하면 FAIL."""
    r = _first(spark,
        f"SELECT count(*) AS bad FROM {CATALOG}.{GOLD_CAMPAIGN} "
        f"WHERE fill_rate NOT BETWEEN 0 AND 1 OR ctr NOT BETWEEN 0 AND 1 OR cvr NOT BETWEEN 0 AND 1")
    bad = r["bad"] or 0
    status = FAIL if bad > 0 else OK
    return [(status, "비율 범위 (gold)", f"범위 이탈 {bad}행 (0이어야 정상)")]


def check_cost_consistency(spark: SparkSession) -> list:
    """cost 정의 교차검증 — gold 총 cost == silver click cost 합 (CPC 모델). 불일치 WARN."""
    r = _first(spark,
        f"SELECT "
        f"(SELECT coalesce(round(sum(cost),2),0) FROM {CATALOG}.{GOLD_CAMPAIGN}) AS gold_cost, "
        f"(SELECT coalesce(round(sum(cost),2),0) FROM {CATALOG}.{SILVER} WHERE event_type='click') AS silver_cost")
    gold_c, silver_c = r["gold_cost"] or 0, r["silver_cost"] or 0
    diff = abs(gold_c - silver_c)
    status = OK if diff < 0.01 else WARN
    return [(status, "cost 정합성", f"gold={gold_c} vs silver_click={silver_c} (차이 {diff:.2f})")]


# ── 리포트 ───────────────────────────────────────────────────────────────────

def run(args) -> None:
    spark = build_spark("pipeline-health")
    spark.sparkContext.setLogLevel("WARN")

    results = []
    results += check_freshness(spark, args.freshness_min)
    results += check_files_and_snapshots(spark, args.min_file_mb, args.max_snapshots)
    results += check_volume(spark)
    results += check_duplicates(spark)
    results += check_funnel_integrity(spark)
    results += check_ratio_range(spark)
    results += check_cost_consistency(spark)

    print("\n" + "=" * 78)
    print(" 파이프라인 헬스체크 리포트")
    print("=" * 78)
    for status, label, detail in results:
        print(f" {ICON[status]} {status:<4} | {label:<32} | {detail}")
    n_fail = sum(1 for s, _, _ in results if s == FAIL)
    n_warn = sum(1 for s, _, _ in results if s == WARN)
    n_ok = len(results) - n_fail - n_warn
    print("=" * 78)
    print(f" 종합: {len(results)}개 체크 | ❌ {n_fail} FAIL · ⚠️ {n_warn} WARN · ✅ {n_ok} OK")
    print("=" * 78 + "\n")

    # FAIL이 하나라도 있으면 비정상 종료 → Airflow/Slack 게이트로 확장 가능
    if n_fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--freshness-min", type=int, default=30,
                   help="신선도 임계(분). 최신 적재/갱신이 이보다 오래되면 WARN. 기본 30.")
    p.add_argument("--min-file-mb", type=float, default=32.0,
                   help="평균 파일 크기 하한(MB). 파일 多 + 평균 미만이면 WARN(컴팩션). 기본 32.")
    p.add_argument("--max-snapshots", type=int, default=200,
                   help="테이블별 스냅샷 누적 임계. 초과 시 WARN(expire). 기본 200.")
    run(p.parse_args())
