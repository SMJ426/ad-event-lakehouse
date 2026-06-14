-- bronze_tables.sql — Bronze 레이어 Iceberg 테이블 정의 (문서/재현용)
--
-- 실제 생성은 bronze_stream.py가 기동 시 CREATE TABLE IF NOT EXISTS로 수행한다.
-- 이 파일은 스키마를 SQL로 명시해 리뷰·재현·문서화하기 위한 것이다.
--
-- 카탈로그: glue (Iceberg + AWS Glue Catalog)
-- 데이터베이스: bronze
-- 토픽 1:1 매핑 → 4개 테이블, 모두 동일 스키마.
--
-- Bronze = raw 원칙:
--   value 컬럼에 Kafka 메시지(JSON)를 파싱 없이 그대로 보존한다.
--   같은 토픽에 dummy(AdEvent)/criteo(CriteoRawEvent) 스키마가 섞이므로
--   파싱은 Silver에서 source 기준으로 분기한다.

CREATE DATABASE IF NOT EXISTS glue.bronze;

-- ad_requests / ad_impressions / ad_clicks / ad_conversions 모두 아래와 동일.

CREATE TABLE IF NOT EXISTS glue.bronze.ad_requests (
    key             string,      -- Kafka 메시지 키 (= campaign_id)
    value           string,      -- raw JSON 원본 (파싱 안 함)
    topic           string,      -- 출처 토픽
    kafka_partition int,         -- Kafka 파티션 번호
    kafka_offset    bigint,      -- Kafka 오프셋 (중복 추적/재처리)
    kafka_timestamp timestamp,   -- 브로커 수신 시각
    ingested_at     timestamp,   -- Bronze 적재 시각 (신선도 측정)
    dt              string,      -- 파티션. kafka_timestamp 기준 yyyy-MM-dd
    hour            int          -- 파티션. kafka_timestamp 기준 0~23
)
USING iceberg
PARTITIONED BY (dt, hour);

CREATE TABLE IF NOT EXISTS glue.bronze.ad_impressions (
    key string, value string, topic string,
    kafka_partition int, kafka_offset bigint,
    kafka_timestamp timestamp, ingested_at timestamp,
    dt string, hour int
)
USING iceberg
PARTITIONED BY (dt, hour);

CREATE TABLE IF NOT EXISTS glue.bronze.ad_clicks (
    key string, value string, topic string,
    kafka_partition int, kafka_offset bigint,
    kafka_timestamp timestamp, ingested_at timestamp,
    dt string, hour int
)
USING iceberg
PARTITIONED BY (dt, hour);

CREATE TABLE IF NOT EXISTS glue.bronze.ad_conversions (
    key string, value string, topic string,
    kafka_partition int, kafka_offset bigint,
    kafka_timestamp timestamp, ingested_at timestamp,
    dt string, hour int
)
USING iceberg
PARTITIONED BY (dt, hour);
