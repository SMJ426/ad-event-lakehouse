-- gold_tables.sql — Gold 레이어 Iceberg 테이블 정의 (문서/재현용)
--
-- 실제 생성은 gold_aggregations.py가 기동 시 CREATE TABLE IF NOT EXISTS로 수행한다.
--
-- 카탈로그: glue / 데이터베이스: gold
-- Gold = Silver(이벤트 단위)를 비즈니스 KPI로 집계한 서빙용 테이블. 대시보드 직접 소스.
--
-- 설계 메모:
--   - 처리: updated_at(Silver 처리시각) 기반 증분 → 변경된 event_date 파티션만 overwritePartitions.
--   - cost: 광고비 = SUM(cost) FILTER(event_type='click') (CPC 모델, criteo/dummy 통일).
--   - ROAS: 매출 데이터가 없어 전환당 가정 단가(GOLD_REVENUE_PER_CONVERSION, 기본 10.0) 사용.
--   - 데이터원 분담: criteo→hourly_funnel(24h 분포), dummy→campaign_daily(일별 추세 누적).

CREATE DATABASE IF NOT EXISTS glue.gold;

-- ① campaign_daily_stats — 캠페인별 일별 성과 (대시보드: 캠페인 성과 탭)
CREATE TABLE IF NOT EXISTS glue.gold.campaign_daily_stats (
    campaign_id   int,
    event_date    date,        -- 파티션
    requests      bigint,
    impressions   bigint,
    clicks        bigint,
    conversions   bigint,
    fill_rate     double,      -- impressions / requests
    ctr           double,      -- clicks / impressions
    cvr           double,      -- conversions / clicks
    cost          double,      -- SUM(cost) WHERE event_type='click'
    cpa           double,      -- cost / conversions
    roas          double,      -- conversions * REVENUE_PER_CONVERSION / cost (가정 단가)
    updated_at    timestamp
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version'='2','write.target-file-size-bytes'='134217728');

-- ② banner_daily_stats — 소재(배너)별 일별 성과 (대시보드: 소재 성과)
CREATE TABLE IF NOT EXISTS glue.gold.banner_daily_stats (
    banner_id     string,
    event_date    date,        -- 파티션
    impressions   bigint,
    clicks        bigint,
    ctr           double,      -- clicks / impressions
    peak_hour     int,         -- 그 banner/일에서 impression 최다 시(hour)
    cost          double,      -- SUM(cost) WHERE event_type='click'
    updated_at    timestamp
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version'='2','write.target-file-size-bytes'='134217728');

-- ③ hourly_funnel — 시간대별 퍼널 (대시보드: 시간대 분포, criteo 24h 강점)
CREATE TABLE IF NOT EXISTS glue.gold.hourly_funnel (
    event_date    date,        -- 파티션
    hour          int,
    requests      bigint,
    impressions   bigint,
    clicks        bigint,
    conversions   bigint,
    fill_rate     double,
    ctr           double,
    cvr           double,
    updated_at    timestamp
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('format-version'='2','write.target-file-size-bytes'='134217728');
