-- silver_tables.sql — Silver 레이어 Iceberg 테이블 정의 (문서/재현용)
--
-- 실제 생성은 silver_processed.py가 기동 시 CREATE TABLE IF NOT EXISTS로 수행한다.
-- 이 파일은 스키마를 SQL로 명시해 리뷰·재현·문서화하기 위한 것이다.
--
-- 카탈로그: glue (Iceberg + AWS Glue Catalog)
-- 데이터베이스: silver
--
-- Silver = 정제 + 통일 + 중복 제거.
--   Bronze raw JSON(dummy AdEvent / criteo CriteoRawEvent 혼재)을 파싱·통일하여
--   이벤트 단위(event_id 1개 = 1 row)로 적재한다.
--
-- 핵심 설계:
--   - grain: 이벤트 단위 (강의식 funnel wide-row는 criteo 40배 폭증이라 회피)
--   - dedup/MERGE 키: event_id
--   - 파티션: event_date (분석·집계가 일 단위, MERGE 대상 파티션 축소)
--   - write mode: COW (배치 MERGE + 분석 읽기 위주 → 읽기 빠른 COW)
--   - format-version 2: row-level UPDATE/DELETE/MERGE 지원 (필수)

CREATE DATABASE IF NOT EXISTS glue.silver;

CREATE TABLE IF NOT EXISTS glue.silver.processed_events (
    event_id              string,      -- dedup/MERGE 키 (이벤트 고유 uuid)
    event_type            string,      -- request | impression | click | conversion
    source                string,      -- dummy | criteo
    auction_id            string,      -- 퍼널 묶음 ID (conversion_delay 계산 join 키)
    campaign_id           int,         -- dummy.campaign_id / criteo.campaign
    event_timestamp       timestamp,   -- 절대 시각 (criteo: BASE + 상대초)
    event_date            date,        -- 파티션. to_date(event_timestamp)
    uid                   string,
    banner_id             string,      -- dummy.banner_id / criteo: campaign_cat1_cat2
    site_cat              string,      -- dummy.site_cat / criteo: cat1 → IAB 매핑
    device_type           int,         -- dummy(1/2/5) / criteo NULL
    os                    string,      -- dummy / criteo NULL
    country               string,      -- dummy / criteo NULL
    cost                  double,      -- dummy.bid_price / criteo.cost*1000 (CPM 통일)
    conversion            int,         -- conversion 이벤트 1, 그 외 0
    conversion_delay_sec  bigint,      -- conversion 이벤트만: conv_ts - 매칭 click_ts
    updated_at            timestamp    -- Silver 적재/갱신 시각
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version' = '2',
    'write.update.mode' = 'copy-on-write',
    'write.merge.mode'  = 'copy-on-write',
    'write.delete.mode' = 'copy-on-write',
    'write.target-file-size-bytes' = '134217728'
);

-- 품질 검증(validation): silver_processed.py가 적재 전 무효 행을 drop한다.
--   규칙: null_event_id / bad_event_type / null_campaign_id / null_uid / negative_cost
--        / timestamp_out_of_range (architecture §5-2).
--   drop된 행은 별도 테이블로 보존하지 않고, 잡 로그에 사유별 건수만 남긴다.
--   (criteo의 정상 NULL device/os/country는 검사 대상이 아님 — 오제거 방지)
