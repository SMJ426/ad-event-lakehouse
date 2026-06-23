"""
iceberg_maintenance.py — Iceberg 테이블 유지보수 배치 잡 (compaction / expire / orphan)

역할:
  Bronze·Silver·Gold Iceberg 테이블의 메타데이터·파일을 정리한다. (연산 로직은 여기 한 곳)
  - compaction(rewrite_data_files): small file를 target 크기로 bin-pack 병합.
  - expire_snapshots: 오래된 스냅샷 + 그에만 속한 데이터/매니페스트 제거.
  - remove_orphan_files: 어떤 스냅샷도 참조하지 않는 고아 파일 제거.

cadence 매핑 (연산 로직은 이 모듈 한 곳, '언제 도는지'는 DAG가 결정):
  - compaction = write-driven(쓰기가 만든 small file 정리) → 메달리온 DAG에서 쓰기 직후 인라인
    (silver/gold). 단 Bronze는 streaming이라 '쓰기 끝' 시점이 없어 GC DAG에서 주기로.
  - expire / orphan = time-driven(시간 지나 쌓인 것 청소) → GC DAG에서 주기로.
    (매 쓰기마다 돌리면 헛바퀴 — 특히 orphan은 매번 테이블 전체 S3 LIST라 비쌈.)
  → DAG는 `--layer/--ops`로 cadence만 wiring한다.

왜 별도 모듈인가:
  적재(write)와 유지보수(maintenance) 로직을 분리한다. 어떤 DAG에서 호출하든 동일 함수 사용.

실행 순서(중요): compaction → expire → orphan
  컴팩션이 새 스냅샷(operation=replace)을 만든 뒤, expire가 구 스냅샷을,
  orphan이 어디서도 참조 안 되는 파일을 회수한다.

동시성(OCC):
  rewrite_data_files는 낙관적 동시성이다. 동시에 MERGE/스트리밍 쓰기가 같은 파티션을
  커밋하면 base 스냅샷이 바뀌어 충돌을 감지하고 검증 실패한다. partial-progress.enabled로
  충돌 안 난 그룹만 부분 커밋하고, 완전 충돌분은 다음 run에서 처리한다.
  remove_orphan_files는 older_than(기본 72h)으로 in-flight 파일 삭제를 방지한다.

실행:
  spark-submit iceberg_maintenance.py --layer all --ops compact,expire,orphan
  (필요 환경변수: S3_BUCKET, AWS_REGION)
"""

import argparse
import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

# ── 환경 설정 ────────────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
WAREHOUSE = f"s3://{S3_BUCKET}/warehouse"
CATALOG = "glue"

# 유지보수 대상 테이블 (카탈로그 접두 제외, "db.table" 형태)
BRONZE_TABLES = [
    "bronze.ad_requests",
    "bronze.ad_impressions",
    "bronze.ad_clicks",
    "bronze.ad_conversions",
]
SILVER_TABLES = ["silver.processed_events"]
GOLD_TABLES = [
    "gold.campaign_daily_stats",
    "gold.banner_daily_stats",
    "gold.hourly_funnel",
]

# 컴팩션 목표 파일 크기 (128MB) — 전 레이어 공통.
TARGET_FILE_SIZE_BYTES = 134_217_728


# ── Spark ────────────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    """Iceberg Glue Catalog + S3FileIO가 설정된 SparkSession 생성.

    bronze_stream / silver_processed와 동일 패턴 (카탈로그 설정 일관성 유지).
    """
    return (
        SparkSession.builder.appName("iceberg-maintenance")
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
        # remove_orphan_files는 테이블 위치(s3://)를 Hadoop FileSystem으로 직접 리스팅한다.
        # S3FileIO는 s3:// 스킴으로 쓰지만 Hadoop엔 s3 스킴 구현이 없으므로 S3A로 매핑.
        # (적재 잡은 s3a 체크포인트만 써서 불필요했으나, 유지보수의 고아 탐색엔 필수)
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ── 메트릭 (전/후 비교용) ─────────────────────────────────────────────────────

def table_metrics(spark: SparkSession, table: str) -> dict:
    """현재 스냅샷의 데이터 파일 수·평균 크기(MB)와 누적 스냅샷 수를 조회.

    .files = 현재 스냅샷의 데이터 파일, .snapshots = 누적 스냅샷 이력.
    """
    files = spark.sql(
        f"SELECT count(*) AS n, "
        f"round(coalesce(avg(file_size_in_bytes), 0) / 1048576.0, 2) AS avg_mb "
        f"FROM {CATALOG}.{table}.files"
    ).first()
    snaps = spark.sql(
        f"SELECT count(*) AS n FROM {CATALOG}.{table}.snapshots"
    ).first()
    return {"files": files["n"], "avg_mb": files["avg_mb"], "snapshots": snaps["n"]}


def log_metrics(label: str, m: dict) -> None:
    print(
        f"[METRIC] {label:<28} files={m['files']:>6} "
        f"avg_size={m['avg_mb']:>8} MB  snapshots={m['snapshots']:>4}"
    )


# ── 유지보수 연산 ─────────────────────────────────────────────────────────────

def compact(spark: SparkSession, table: str, dry_run: bool) -> None:
    """rewrite_data_files — small file를 target 크기로 bin-pack 병합."""
    sql = (
        f"CALL {CATALOG}.system.rewrite_data_files("
        f"table => '{table}', "
        f"options => map("
        f"'min-input-files', '5', "
        f"'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}', "
        f"'partial-progress.enabled', 'true', "
        f"'partial-progress.max-commits', '10'))"
    )
    print(f"[COMPACT] {table}\n          {sql}")
    if dry_run:
        print("[COMPACT] dry-run — 실행 생략")
        return
    spark.sql(sql).show(truncate=False)


def expire(spark: SparkSession, table: str, retention_days: int, dry_run: bool) -> None:
    """expire_snapshots — older_than 이전 스냅샷 제거(단 최근 retain_last개는 보존)."""
    older_than = (datetime.now(timezone.utc) - timedelta(days=retention_days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    sql = (
        f"CALL {CATALOG}.system.expire_snapshots("
        f"table => '{table}', "
        f"older_than => TIMESTAMP '{older_than}', "
        f"retain_last => 5)"
    )
    print(f"[EXPIRE]  {table} (older_than={older_than}, retain_last=5)\n          {sql}")
    if dry_run:
        print("[EXPIRE]  dry-run — 실행 생략")
        return
    spark.sql(sql).show(truncate=False)


def orphan(spark: SparkSession, table: str, orphan_age_hours: int, dry_run: bool) -> None:
    """remove_orphan_files — 어떤 스냅샷도 참조하지 않는 고아 파일 제거.

    older_than으로 in-flight 파일(스트리밍이 쓰는 중)을 보호한다.
    """
    # Iceberg 안전장치: older_than이 24h 미만이면 in-flight 파일 손상 위험으로 차단된다.
    # 자동화가 잘못된 인자로 크래시하지 않게 24h로 클램프하고 경고.
    if orphan_age_hours < 24:
        print(f"[ORPHAN]  WARN: orphan_age_hours={orphan_age_hours} < 24 → 24로 클램프 "
              f"(Iceberg가 24h 미만을 차단함).")
        orphan_age_hours = 24
    older_than = (
        datetime.now(timezone.utc) - timedelta(hours=orphan_age_hours)
    ).strftime("%Y-%m-%d %H:%M:%S")
    sql = (
        f"CALL {CATALOG}.system.remove_orphan_files("
        f"table => '{table}', "
        f"older_than => TIMESTAMP '{older_than}')"
    )
    print(f"[ORPHAN]  {table} (older_than={older_than})\n          {sql}")
    if dry_run:
        print("[ORPHAN]  dry-run — 실행 생략")
        return
    spark.sql(sql).show(truncate=False)


# ── 메인 ─────────────────────────────────────────────────────────────────────

def maintain_table(spark: SparkSession, table: str, ops: list[str], args) -> None:
    print(f"\n=== 유지보수 시작: {table} | ops={ops} ===")
    log_metrics(f"{table} (before)", table_metrics(spark, table))

    # 순서 보장: compaction → expire → orphan
    if "compact" in ops:
        compact(spark, table, args.dry_run)
    if "expire" in ops:
        expire(spark, table, args.snapshot_retention_days, args.dry_run)
    if "orphan" in ops:
        orphan(spark, table, args.orphan_age_hours, args.dry_run)

    log_metrics(f"{table} (after)", table_metrics(spark, table))
    print(f"=== 유지보수 완료: {table} ===")


def run(args) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    tables: list[str] = []
    if args.layer in ("bronze", "all"):
        tables += BRONZE_TABLES
    if args.layer in ("silver", "all"):
        tables += SILVER_TABLES
    if args.layer in ("gold", "all"):
        tables += GOLD_TABLES

    ops = [o.strip() for o in args.ops.split(",") if o.strip()]
    print(
        f"[INFO] Iceberg 유지보수 | layer={args.layer} | tables={len(tables)} | "
        f"ops={ops} | dry_run={args.dry_run}"
    )

    for table in tables:
        maintain_table(spark, table, ops, args)

    print("\n[INFO] 전체 유지보수 완료.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all",
                   help="유지보수 대상 레이어. all=bronze+silver+gold. 기본 all.")
    p.add_argument("--ops", default="compact,expire,orphan",
                   help="실행 연산(쉼표 구분): compact,expire,orphan. 기본 전체.")
    p.add_argument("--snapshot-retention-days", type=int, default=7,
                   help="expire_snapshots 보존 기간(일). 이보다 오래된 스냅샷 제거. 기본 7.")
    p.add_argument("--orphan-age-hours", type=int, default=72,
                   help="remove_orphan_files older_than(시간). in-flight 파일 보호. 기본 72.")
    p.add_argument("--dry-run", action="store_true",
                   help="실제 CALL 실행 없이 계획·메트릭만 출력.")
    run(p.parse_args())
