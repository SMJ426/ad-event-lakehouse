-- trino_queries.sql — Superset 차트용 쿼리 (Trino → Iceberg)
--
-- Superset SQL Lab에서 데이터베이스 "Iceberg (Trino)" 선택 후 실행.
-- 카탈로그=iceberg / 스키마=bronze|silver|gold. 메타테이블은 "table$files" 형태(따옴표 필수).
--
-- ════════════════════════════════════════════════════════════════════════
-- 탭 A — 비즈니스 KPI (Gold)
-- ════════════════════════════════════════════════════════════════════════

-- A1. 전체 KPI 빅넘버 (대시보드 상단 타일)
SELECT
    sum(impressions)                                   AS impressions,
    sum(clicks)                                        AS clicks,
    sum(conversions)                                   AS conversions,
    round(sum(cost), 2)                                AS cost,
    round(sum(clicks)      * 1.0 / nullif(sum(impressions),0), 4) AS ctr,
    round(sum(conversions) * 1.0 / nullif(sum(clicks),0),      4) AS cvr,
    round(sum(conversions) * 10.0 / nullif(sum(cost),0),       2) AS roas   -- 가정 단가 $10
FROM iceberg.gold.campaign_daily_stats;

-- A2. 캠페인 성과 Top 20 (cost 기준) — 테이블 차트
SELECT campaign_id, event_date, impressions, clicks, conversions,
       ctr, cvr, cost, cpa, roas
FROM iceberg.gold.campaign_daily_stats
ORDER BY cost DESC
LIMIT 20;

-- A3. 시간대 퍼널 (criteo 24h 분포) — 라인/바 차트 (x=hour)
SELECT event_date, hour, requests, impressions, clicks, conversions,
       fill_rate, ctr, cvr
FROM iceberg.gold.hourly_funnel
ORDER BY event_date, hour;

-- A4. 소재(배너) 성과 Top 20 (CTR 기준, 노출 100+) — 테이블 차트
SELECT banner_id, event_date, impressions, clicks, ctr, peak_hour, cost
FROM iceberg.gold.banner_daily_stats
WHERE impressions >= 100
ORDER BY ctr DESC
LIMIT 20;

-- ════════════════════════════════════════════════════════════════════════
-- 탭 B — 운영 메트릭 (평가기준 ① 운영 가시성 / 강의 p.19)
-- ════════════════════════════════════════════════════════════════════════

-- B1. 신선도 — 레이어별 최신 시각 (silver/gold = updated_at, bronze = 최신 스냅샷)
SELECT 'silver.processed_events' AS tbl, cast(max(updated_at) AS varchar) AS latest FROM iceberg.silver.processed_events
UNION ALL SELECT 'gold.campaign_daily_stats', cast(max(updated_at) AS varchar) FROM iceberg.gold.campaign_daily_stats
UNION ALL SELECT 'bronze.ad_clicks (snapshot)', cast(max(committed_at) AS varchar) FROM iceberg.bronze."ad_clicks$snapshots";

-- B2. 일자별 행 수 — Bronze/Silver (이상 트래픽 감지)
SELECT 'bronze.ad_clicks' AS tbl, dt AS d, count(*) AS rows FROM iceberg.bronze.ad_clicks GROUP BY dt
UNION ALL
SELECT 'silver.processed_events', cast(event_date AS varchar), count(*) FROM iceberg.silver.processed_events GROUP BY event_date
ORDER BY tbl, d;

-- B3. 중복 상태 — event_id 중복 수 (0이어야 정상)
SELECT count(*) - count(DISTINCT event_id) AS duplicate_event_ids
FROM iceberg.silver.processed_events;

-- B4. 파일 상태 (Small File) — 테이블별 파일 수 / 평균 크기 (작으면 컴팩션 필요)
SELECT 'bronze.ad_requests'  AS tbl, count(*) AS files, round(avg(file_size_in_bytes)/1048576.0,2) AS avg_mb FROM iceberg.bronze."ad_requests$files"
UNION ALL SELECT 'bronze.ad_impressions', count(*), round(avg(file_size_in_bytes)/1048576.0,2) FROM iceberg.bronze."ad_impressions$files"
UNION ALL SELECT 'bronze.ad_clicks',      count(*), round(avg(file_size_in_bytes)/1048576.0,2) FROM iceberg.bronze."ad_clicks$files"
UNION ALL SELECT 'silver.processed_events', count(*), round(avg(file_size_in_bytes)/1048576.0,2) FROM iceberg.silver."processed_events$files"
UNION ALL SELECT 'gold.campaign_daily_stats', count(*), round(avg(file_size_in_bytes)/1048576.0,2) FROM iceberg.gold."campaign_daily_stats$files";

-- B5. 스냅샷 누적 수 — 테이블별 (만료 필요 여부)
SELECT 'bronze.ad_clicks' AS tbl, count(*) AS snapshots FROM iceberg.bronze."ad_clicks$snapshots"
UNION ALL SELECT 'silver.processed_events', count(*) FROM iceberg.silver."processed_events$snapshots"
UNION ALL SELECT 'gold.campaign_daily_stats', count(*) FROM iceberg.gold."campaign_daily_stats$snapshots";
