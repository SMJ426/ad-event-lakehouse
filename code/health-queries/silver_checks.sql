-- silver_checks.sql — Silver processed_events 정제 검증 쿼리
--
-- Athena(서빙 단계) 또는 spark-sql로 실행. DB는 glue.silver / Athena에선 silver.
-- 카탈로그/DB 접두는 실행 엔진에 맞게 조정.

-- ── 1. source × event_type 분포 (통일 + 두 원천 공존 확인) ───────────────────
SELECT source, event_type, count(*) AS cnt
FROM silver.processed_events
GROUP BY source, event_type
ORDER BY source, event_type;

-- ── 2. event_id 중복 0 확인 (dedup 동작) ────────────────────────────────────
SELECT count(*) - count(DISTINCT event_id) AS duplicate_event_ids
FROM silver.processed_events;

-- ── 3. event_date 파티션 분포 ───────────────────────────────────────────────
SELECT event_date, event_type, count(*) AS cnt
FROM silver.processed_events
GROUP BY event_date, event_type
ORDER BY event_date, event_type;

-- ── 4. conversion_delay_sec 분포 (메커니즘 동작 — 시뮬레이터상 값은 ≈0) ──────
SELECT
    count(*)                              AS conversions,
    count(conversion_delay_sec)           AS delay_filled,   -- click 매칭된 수
    min(conversion_delay_sec)             AS min_delay_sec,
    avg(conversion_delay_sec)             AS avg_delay_sec,
    max(conversion_delay_sec)             AS max_delay_sec
FROM silver.processed_events
WHERE event_type = 'conversion';

-- ── 5. 통일 스키마 샘플 (criteo가 OpenRTB로 변환됐는지 육안 확인) ────────────
SELECT event_id, source, event_type, campaign_id, banner_id, site_cat,
       cost, event_timestamp, conversion, conversion_delay_sec
FROM silver.processed_events
WHERE source = 'criteo'
LIMIT 10;

-- ── 6. Iceberg 스냅샷 (MERGE 동작 — operation=overwrite, 멱등 재실행 확인) ────
SELECT committed_at, operation,
       summary['added-records']   AS added,
       summary['deleted-records'] AS deleted
FROM silver.processed_events.snapshots
ORDER BY committed_at DESC
LIMIT 10;

-- ── 7. 품질 검증(validation) 관측 ───────────────────────────────────────────
-- 무효 행은 silver_processed.py가 적재 전 drop하고, 사유별 건수는 잡 로그에 남긴다
-- (별도 격리 테이블 없음). Airflow task 로그의 "validation 완료 — drop된 행 분포"에서 확인.
-- 적재 결과 측면의 품질은 아래로 점검:
--   (a) NULL 키가 통과 안 했는지 (기대 0)
SELECT count(*) AS null_key_rows
FROM silver.processed_events
WHERE event_id IS NULL OR campaign_id IS NULL OR uid IS NULL;
--   (b) 음수 cost가 통과 안 했는지 (기대 0)
SELECT count(*) AS negative_cost_rows
FROM silver.processed_events
WHERE cost < 0;
