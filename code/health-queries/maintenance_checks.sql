-- maintenance_checks.sql — Iceberg 유지보수(컴팩션/만료/고아) 검증·헬스 쿼리
--
-- spark-sql 또는 Athena로 실행. 카탈로그/DB 접두는 엔진에 맞게 조정
-- (spark-sql: glue.bronze.* / Athena: bronze.*). 아래는 spark-sql 기준.
--
-- 운영자 5분 헬스체크: 1·2번으로 small file / 스냅샷 누적을 매일 확인하고,
-- 유지보수 전후로 3·4번을 비교해 효과를 검증한다.

-- ── 1. small file 감지 — 테이블별 파일 수 / 평균 크기 ─────────────────────────
-- avg_size_mb가 target(128MB)보다 한참 작고 file_count가 크면 컴팩션 필요.
SELECT 'bronze.ad_requests'    AS tbl, count(*) AS files, round(avg(file_size_in_bytes)/1048576.0,2) AS avg_mb FROM glue.bronze.ad_requests.files
UNION ALL SELECT 'bronze.ad_impressions', count(*), round(avg(file_size_in_bytes)/1048576.0,2) FROM glue.bronze.ad_impressions.files
UNION ALL SELECT 'bronze.ad_clicks',      count(*), round(avg(file_size_in_bytes)/1048576.0,2) FROM glue.bronze.ad_clicks.files
UNION ALL SELECT 'bronze.ad_conversions', count(*), round(avg(file_size_in_bytes)/1048576.0,2) FROM glue.bronze.ad_conversions.files
UNION ALL SELECT 'silver.processed_events', count(*), round(avg(file_size_in_bytes)/1048576.0,2) FROM glue.silver.processed_events.files;

-- ── 2. 스냅샷 누적 수 (만료 필요 여부) ───────────────────────────────────────
-- 계속 증가하면 메타데이터 비대 → expire_snapshots 필요.
SELECT 'bronze.ad_clicks' AS tbl, count(*) AS snapshots FROM glue.bronze.ad_clicks.snapshots
UNION ALL SELECT 'silver.processed_events', count(*) FROM glue.silver.processed_events.snapshots;

-- ── 3. 컴팩션 동작 확인 — operation=replace 스냅샷 (rewrite_data_files 흔적) ──
SELECT committed_at, operation,
       summary['added-data-files']   AS added_files,
       summary['deleted-data-files'] AS deleted_files
FROM glue.bronze.ad_clicks.snapshots
ORDER BY committed_at DESC
LIMIT 10;

-- ── 4. 파티션별 파일 분포 (어느 파티션이 잘게 쪼개졌는지) ─────────────────────
SELECT partition, count(*) AS files,
       round(avg(file_size_in_bytes)/1048576.0,2) AS avg_mb,
       sum(record_count) AS rows
FROM glue.bronze.ad_clicks.files
GROUP BY partition
ORDER BY files DESC
LIMIT 20;

-- ── 5. 데이터 무손실 확인 — 유지보수 전후 행 수 불변 ─────────────────────────
-- 컴팩션/만료/고아 제거는 메타·파일만 정리할 뿐 레코드는 보존돼야 한다.
SELECT count(*) AS rows FROM glue.silver.processed_events;

-- ── 6. 메타데이터 파일 수 (manifest 누적 — rewrite_manifests 필요 여부 참고) ──
SELECT count(*) AS manifest_files FROM glue.bronze.ad_clicks.manifests;
