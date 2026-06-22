-- gold_checks.sql — Gold KPI 테이블 검증/헬스 쿼리
--
-- spark-sql 또는 Athena로 실행. (spark-sql: glue.gold.* / Athena: gold.*)
-- 아래는 spark-sql 기준.

-- ── 1. 테이블 행수 + event_date 커버리지 ────────────────────────────────────
SELECT 'campaign_daily_stats' AS tbl, count(*) AS rows, count(DISTINCT event_date) AS dates FROM glue.gold.campaign_daily_stats
UNION ALL SELECT 'banner_daily_stats', count(*), count(DISTINCT event_date) FROM glue.gold.banner_daily_stats
UNION ALL SELECT 'hourly_funnel',      count(*), count(DISTINCT event_date) FROM glue.gold.hourly_funnel;

-- ── 2. 퍼널 단조성 위반 탐지 (requests≥impressions≥clicks≥conversions 이어야) ─
-- 결과가 0행이면 정상. (criteo 합성비 50:40:1 + dummy 퍼널이라 성립해야 함)
SELECT campaign_id, event_date, requests, impressions, clicks, conversions
FROM glue.gold.campaign_daily_stats
WHERE impressions > requests OR clicks > impressions OR conversions > clicks;

-- ── 3. 비율 범위 위반 탐지 (0≤fill_rate,ctr,cvr≤1 이어야) ────────────────────
-- 결과가 0행이면 정상.
SELECT campaign_id, event_date, fill_rate, ctr, cvr
FROM glue.gold.campaign_daily_stats
WHERE fill_rate > 1 OR ctr > 1 OR cvr > 1
   OR fill_rate < 0 OR ctr < 0 OR cvr < 0;

-- ── 4. 캠페인 성과 Top 10 (비즈니스 KPI 미리보기) ───────────────────────────
SELECT campaign_id, event_date, impressions, clicks, conversions,
       ctr, cvr, cost, cpa, roas
FROM glue.gold.campaign_daily_stats
ORDER BY cost DESC
LIMIT 10;

-- ── 5. 시간대 퍼널 (criteo 24h 분포 — hourly_funnel) ────────────────────────
SELECT event_date, hour, requests, impressions, clicks, conversions, fill_rate, ctr, cvr
FROM glue.gold.hourly_funnel
ORDER BY event_date, hour;

-- ── 6. 신선도 — 마지막 집계 시각 (운영 메트릭) ───────────────────────────────
SELECT max(updated_at) AS last_gold_update FROM glue.gold.campaign_daily_stats;

-- ── 7. cost 정의 교차검증 — Gold cost == Silver click cost 합 ────────────────
-- 두 값이 일치하면 "cost=click 기준" 정의가 맞게 적용된 것.
SELECT
    (SELECT round(sum(cost),2) FROM glue.gold.campaign_daily_stats)                          AS gold_cost,
    (SELECT round(sum(cost),2) FROM glue.silver.processed_events WHERE event_type='click')   AS silver_click_cost;
