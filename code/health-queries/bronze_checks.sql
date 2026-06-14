-- bronze_checks.sql — Bronze 적재 검증/헬스 쿼리
--
-- ⚠️ 이 쿼리들은 이후 "서빙 단계"에서 Athena로 실행하기 위한 것이다.
--    Bronze 적재 자체의 검증은 `aws s3 ls` + Spark count로 충분하다.
--    (Athena 워크그룹/결과 위치 설정이 선행되어야 실행 가능)
--
-- 카탈로그/DB: Athena에서는 데이터베이스 'bronze'로 접근.

-- ── 1. 토픽별 적재 건수 ─────────────────────────────────────────────────────
SELECT 'ad_requests'    AS table_name, count(*) AS cnt FROM bronze.ad_requests
UNION ALL SELECT 'ad_impressions', count(*) FROM bronze.ad_impressions
UNION ALL SELECT 'ad_clicks',      count(*) FROM bronze.ad_clicks
UNION ALL SELECT 'ad_conversions', count(*) FROM bronze.ad_conversions;

-- ── 2. source별(dummy/criteo) 건수 — value(JSON)에서 추출 ───────────────────
-- raw 보존이라 value가 JSON 문자열. json_extract_scalar로 source 필드만 꺼낸다.
SELECT json_extract_scalar(value, '$.source') AS source, count(*) AS cnt
FROM bronze.ad_clicks
GROUP BY json_extract_scalar(value, '$.source');

-- ── 3. dt/hour 파티션 분포 ──────────────────────────────────────────────────
SELECT dt, hour, count(*) AS cnt
FROM bronze.ad_clicks
GROUP BY dt, hour
ORDER BY dt, hour;

-- ── 4. Bronze 신선도 — 마지막 적재 시각 (Iceberg 스냅샷 메타) ────────────────
SELECT committed_at, summary['added-records'] AS added_rows
FROM bronze.ad_clicks.snapshots
ORDER BY committed_at DESC
LIMIT 5;

-- ── 5. small file 감지 — 파일 수 / 평균 크기 (Iceberg files 메타) ────────────
SELECT count(*) AS file_count,
       round(avg(file_size_in_bytes) / 1024.0 / 1024.0, 2) AS avg_size_mb
FROM bronze.ad_clicks.files;
