# 광고 이벤트 레이크하우스 — 아키텍처 설계

## 1. 프로젝트 개요

| 항목 | 내용 |
|------|------|
| 도메인 | 광고 이벤트 (Ad Event) |
| 원천 데이터 | Criteo Attribution Dataset + 자체 웹 이벤트 |
| 데이터 규모 | 일 100만 이벤트 → 6개월 내 10x 성장 가정 |
| 핵심 목표 | 실시간 광고 이벤트 수집 → 메달리온 아키텍처 → KPI 대시보드 |

---

## 2. 전체 아키텍처

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            K8s (EKS)                                    │
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐                                   │
│  │  웹 배너 페이지 │    │ Criteo CSV    │                                  │
│  │  (실시간)     │    │ (재생 시뮬레이션)│                                   │
│  └──────┬───────┘    └──────┬───────┘                                   │
│         │                   │                                           │
│         ▼                   ▼                                           │
│  ┌──────────────┐    ┌──────────────┐                                   │
│  │ Event        │    │ Kafka        │                                   │
│  │ Collector API│    │ Producer     │                                   │
│  └──────┬───────┘    └──────┬───────┘                                   │
│         │                   │                                           │
│         └──────────┬────────┘                                           │
│                    ▼                                                    │
│         ┌─────────────────────┐                                         │
│         │       Kafka         │                                         │
│         │  ┌───────────────┐  │                                         │
│         │  │ ad-requests   │  │                                         │
│         │  │ ad-impressions│  │                                         │
│         │  │ ad-clicks     │  │                                         │
│         │  │ ad-conversions│  │                                         │
│         │  └───────────────┘  │                                         │
│         └──────────┬──────────┘                                         │
│                    ▼                                                    │
│         ┌─────────────────────┐                                         │
│         │ Spark Structured    │                                         │
│         │ Streaming           │                                         │
│         └──────────┬──────────┘                                         │
│                    │                                                    │
└────────────────────┼────────────────────────────────────────────────────┘
                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           AWS                                           │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │                    AWS S3 (Apache Iceberg)                      │   │
│   │                                                                 │   │
│   │   Bronze (raw)  ──→  Silver (정제)  ──→  Gold (집계 KPI)          │   │
│   │                                                                 │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                    ▲                    ▲                               │
│                    └────────────────────┘                               │
│                         Airflow → AWS Glue ETL                          │
│                                                                         │
│              AWS Glue Catalog (테이블 메타데이터)                            │
│                         ▼                                               │
│                    AWS Athena  ──→  AWS QuickSight(이외 툴 가능)           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. 데이터 소스

> **OpenRTB 2.6 표준 참고:** 이벤트 스키마는 실제 광고 업계 표준인 [OpenRTB 2.6](https://iabtechlab.com/wp-content/uploads/2022/04/OpenRTB-2-6_FINAL.pdf) 을 기반으로 설계한다.

### 3-1. 더미 데이터 생성기 (주요 볼륨 소스)

OpenRTB 표준을 참고한 더미 데이터를 대량으로 생성하는 스크립트. 파이프라인의 **주요 볼륨 소스**로 사용한다.

```python
# dummy_generator.py
# 초당 N건의 광고 이벤트를 실시간처럼 지속 생성

while True:
    for _ in range(EVENTS_PER_SECOND):
        event = generate_openrtb_event(
            campaigns=CAMPAIGN_POOL,    # 사전 정의된 캠페인 풀
            banners=BANNER_POOL,        # 사전 정의된 배너 풀
            devices=DEVICE_TYPES,       # mobile / pc / tablet
        )
        producer.send(topic(event), event)
    time.sleep(1)
```

**웹 이벤트 대비 장점:**
- 초당 수천 건 생성 가능 → 대용량 파이프라인 검증
- 파라미터 조절로 트래픽 패턴 자유롭게 시뮬레이션
- 피크 타임, 이상 트래픽 등 시나리오 재현 가능

### 3-2. 웹 이벤트 (실시간 실증)

자체 구축한 광고 배너 웹페이지에서 발생하는 진짜 실시간 이벤트. **소량이지만 실제 실시간 경로가 동작함을 증명**하는 용도.

```
페이지 로드
    ↓
JS → Event Collector API 로 request 이벤트 전송
    ↓
배너 렌더링 성공 시 → impression 이벤트 전송
배너 렌더링 실패 시 → impression 미발생 (이탈, 광고차단 등)
    ↓
유저 클릭 시 → click 이벤트 전송
    ↓
전환 도달 시 → conversion 이벤트 전송
```

> **웹 Fill Rate < 100%**: 페이지 이탈, Ad Blocker, 네트워크 지연 등으로 request 발생 후 impression이 찍히지 않는 경우 존재.

### 3-3. Criteo Attribution Dataset (백필)

[Criteo Attribution Dataset](https://huggingface.co/datasets/criteo/criteo-attribution-dataset)을 Kafka Producer로 실시간처럼 재생.

- 총 약 1,600만 건의 클릭 이벤트

**이벤트 합성 로직:**

Criteo는 `click`과 `conversion` 데이터만 존재. `request`와 `impression`은 업계 평균 지표를 역산하여 합성 생성.

```
가정 지표:
  Fill Rate = 80%   (impression / request)
  CTR       = 2.5%  (click / impression)

역산:
  click 1건 기준
    → impression = 1 / 0.025 = 40건
    → request    = 40 / 0.8  = 50건

결과: 클릭 1건당 request 50건, impression 40건 합성 생성
```

**campaign_id vs banner_id:**

| 필드 | 설명 | 관계 |
|------|------|------|
| `campaign_id` | 마케팅 목표 단위 (예산, 기간, 타겟) | 1 |
| `banner_id` | 실제 유저에게 노출되는 광고 소재 단위 | N |

하나의 캠페인은 여러 배너를 가질 수 있다.
```
campaign_id: 12345678
  └── banner: 12345678_A_X  (PC 메인용 소재)
  └── banner: 12345678_A_Y  (모바일용 소재)
  └── banner: 12345678_B_Z  (사이드바용 소재)
```

**campaign_id:** Criteo의 `campaign` 컬럼이 int64 고유 ID이므로 그대로 사용.

```python
campaign_id = row.campaign   # int64 고유값 그대로 사용 (파생 불필요)
```

**banner_id:** Criteo에 없는 컬럼이므로 파생 필요. 같은 캠페인 내 다른 소재를 구분하기 위해 `cat` 컬럼 조합 사용.

```python
banner_id = f"{row.campaign}_{row.cat1}_{row.cat2}"  # 파생 필요
# 예) 12345678_A_X
```

---

## 4. Kafka Topic 설계 - 아직 미정

| Topic | 파티션 키 | 이벤트 설명 |
|-------|----------|-------------|
| `ad-requests` | `campaign_id` | 광고 슬롯 요청 |
| `ad-impressions` | `campaign_id` | 광고 실제 노출 |
| `ad-clicks` | `campaign_id` | 유저 클릭 |
| `ad-conversions` | `campaign_id` | 구매 전환 |

**공통 이벤트 스키마** (OpenRTB 2.6 기반, 추후 개발하면서 변경될 수 있음)

```json
{
  "event_id":    "uuid4",                  // 이벤트 고유 ID
  "event_type":  "request",               // request | impression | click | conversion
  "source":      "dummy | web | criteo",  // 데이터 출처 구분

  // OpenRTB BidRequest 기반
  "auction_id":  "uuid4",                 // 경매 단위 묶음 ID (BidRequest.id)
  "banner_id":   "BANNER_001",
  "campaign_id": 12345678,

  // OpenRTB Banner 기반
  "banner_w":    300,                     // 배너 너비 (px)
  "banner_h":    250,                     // 배너 높이 (px)
  "banner_pos":  1,                       // 노출 위치 (1=above fold, 3=below fold)

  // OpenRTB Site 기반
  "site_domain": "news.example.com",
  "site_cat":    "IAB12",                 // IAB 카테고리

  // OpenRTB Device 기반
  "device_type": 1,                       // 1=mobile, 2=pc, 5=tablet
  "os":          "android",
  "country":     "KR",

  // OpenRTB User 기반
  "uid":         "user_abc123",           // User.id

  // 광고 비용
  "floor_price": 0.5,                    // 최소 입찰가 (CPM, USD)
  "bid_price":   0.8,                    // 낙찰 금액

  "timestamp":   1456790400,
  "produced_at": "2024-01-15T10:00:00Z"
}
```

> `source` 필드로 dummy / web / criteo 이벤트를 구분한다.


---

## 5. 메달리온 아키텍처

### 5-1. Bronze (Raw Zone)

**목적:** 원본 데이터를 변환 없이 영구 보존.

| 항목 | 내용 |
|------|------|
| 처리 방식 | Spark Structured Streaming |
| 저장 형식 | Apache Iceberg |
| 파티션 | `dt` (날짜), `hour` (시간) |
| 변환 | 없음 (파티션 컬럼 추가만) |

```
s3://bucket/bronze/
  ad_requests/dt=2024-01-15/hour=10/part-0001.parquet
  ad_impressions/dt=2024-01-15/hour=10/part-0001.parquet
  ad_clicks/dt=2024-01-15/hour=10/part-0001.parquet
  ad_conversions/dt=2024-01-15/hour=10/part-0001.parquet
```

### 5-2. Silver (Processed Zone)

**목적:** 신뢰할 수 있는 정제 데이터 제공.

| 항목 | 내용 |
|------|------|
| 처리 방식 | AWS Glue ETL (Spark Batch) |
| 스케줄 | 매 정시 (Airflow) |
| 처리 내용 | 중복 제거, 이상값 제거, 스키마 통일 |

**정제 규칙:**

```
- event_id 기준 중복 이벤트 제거
- cost < 0 인 레코드 제거
- timestamp 범위 이상값 제거
- uid / campaign_id / banner_id null 제거
```

```
s3://bucket/silver/
  ad_events/dt=2024-01-15/part-0001.parquet
```

### 5-3. Gold (Summary Zone)

**목적:** 비즈니스 KPI 집계. 대시보드 직접 연결.

| 항목 | 내용 |
|------|------|
| 처리 방식 | AWS Glue ETL (Spark Batch) |
| 스케줄 | Silver 완료 후 즉시 (Airflow) |

**Gold 테이블 목록:**

**① campaign_daily_stats** — 캠페인별 일별 성과
```
campaign_id | dt         | request | impression | click | conversion | CTR  | CVR  | fill_rate | cost
CAMP_042    | 2024-01-15 | 50,000  | 40,000     | 1,000 | 50         | 2.5% | 5.0% | 80%       | $450
```

**② banner_daily_stats** — 배너별 일별 성과
```
banner_id     | dt         | impression | click | CTR  | peak_hour
CAMP_042_A_X  | 2024-01-15 | 22,000     | 594   | 2.7% | 14
```

**③ hourly_funnel** — 시간대별 퍼널
```
dt         | hour | request | impression | click | conversion | fill_rate | CTR  | CVR
2024-01-15 | 10   | 8,000   | 6,400      | 160   | 8          | 80%       | 2.5% | 5%
```

더 추가 예정

---

## 6. 핵심 KPI

### 비즈니스 KPI

| KPI | 계산식 | 설명 |
|-----|--------|------|
| Fill Rate | impression / request | 광고 슬롯이 채워진 비율 |
| CTR | click / impression | 클릭률 |
| CVR | conversion / click | 전환율 |
| CPA | cost / conversion | 전환당 비용 |
| ROAS | conversion × 단가 / cost | 광고비 대비 매출 |

### 운영 메트릭

| 메트릭 | 설명 |
|--------|------|
| Bronze 신선도 | 마지막 적재 시각 |
| 시간당 이벤트 수 | 이상 트래픽 감지 |
| Iceberg 스냅샷 수 | 컴팩션 필요 여부 판단 |
| Iceberg 파일 평균 크기 | Small file 문제 감지 |

---

## 7. 기술 스택

| 구성 요소 | 기술 | 실행 위치 |
|----------|------|----------|
| 메시지 큐 | Apache Kafka | 로컬 Docker |
| 스트리밍 처리 | Spark Structured Streaming | K8s (EKS) |
| 배치 처리 | AWS Glue ETL | AWS |
| 오케스트레이션 | Apache Airflow | K8s (EKS) |
| 스토리지 | AWS S3 | AWS |
| 테이블 형식 | Apache Iceberg | AWS S3 위 |
| 메타데이터 카탈로그 | AWS Glue Catalog | AWS |
| 쿼리 엔진 | AWS Athena | AWS |
| 대시보드 | 별도 오픈소스| -- |
| 인프라 | K8s (EKS) | AWS |

> ⚠️ **Spark on K8s 구현 난이도 주의**: Spark on EKS는 구현 난이도가 매우 높다. 초기에는 로컬 Docker 또는 AWS Glue ETL로 파이프라인을 먼저 완성한 뒤, K8s 전환을 단계적으로 진행하는 것을 권장한다.

---

## 8. 이벤트 발생 비율

### 웹 이벤트 — 유저 1명 기준 퍼널

```
유저 1명이 페이지 접속
  → request    1건 발생  (항상)
  → impression 0~1건    (이탈·광고차단 시 미발생, fill rate < 100%)
  → click      0~1건    (impression 발생한 경우에만 가능)
  → conversion 0~1건    (click 발생한 경우에만 가능)
```

### Criteo 재생 — 클릭 1건 기준 역산

Criteo 원본에는 click과 conversion만 존재. request와 impression은 업계 평균으로 역산하여 합성 생성. (Producer가 Kafka에 넣는 시점에 스크립트로 생성)

```
click 1건이 나오기까지 앞에 있었을 이벤트:
  request    50건  →  (fill rate 80%)  →  impression 40건  →  (CTR 2.5%)  →  click 1건
                                                                               ↓
                                                                         conversion 0~1건
```

### 일별 Kafka 토픽 이벤트 규모

하루 클릭 100만 건(Criteo 기준) 처리 시 각 토픽에 쌓이는 메시지 수:

```
클릭 100만건 × 50  =  ad-requests    약 5,000만 건
클릭 100만건 × 40  =  ad-impressions 약 4,000만 건
클릭 100만건 × 1   =  ad-clicks      약   100만 건
클릭 100만건 × 3.5% =  ad-conversions 약    3.5만 건
```

> 웹 이벤트는 실제 유저가 소수이므로 전체 볼륨의 대부분은 Criteo 재생 데이터가 차지한다.

---

## 9. 운영 가시성

> 파이프라인이 잘 설계됐어도, 운영 중 문제를 빠르게 감지하고 대응하지 못하면 의미가 없다.
> **"운영자가 매일 5분 안에 파이프라인 헬스체크를 할 수 있는가"** 를 기준으로 설계한다.

### 발생 가능한 문제들

| 문제 | 증상 | 원인 |
|------|------|------|
| Spark Streaming 중단 | Bronze 신선도 저하 | OOM, Pod 재시작 |
| Kafka 적체 | Consumer lag 증가 | Spark 처리 속도 < 유입 속도 |
| 더미 생성기 중단 | 이벤트 수 급감 | 스크립트 오류 |
| Silver 버그 | 집계 KPI 이상 | 정제 로직 오류 |
| 중복 이벤트 급증 | event_id 중복률 증가 | Producer 재시작 중복 발행 |

### Iceberg 메타 테이블 기반 헬스 쿼리

Iceberg는 테이블 상태를 조회할 수 있는 메타 테이블을 제공한다.

```sql
-- 1. Bronze 신선도 확인 (마지막 적재 시각)
SELECT committed_at, summary['added-records'] AS added_rows
FROM bronze.ad_clicks.snapshots
ORDER BY committed_at DESC LIMIT 1;

-- 2. 시간당 이벤트 수 이상 감지
SELECT hour, COUNT(*) AS cnt
FROM bronze.ad_clicks
WHERE dt = current_date
GROUP BY hour ORDER BY hour;

-- 3. Iceberg 파일 수 / 평균 크기 확인 (small file 감지)
SELECT COUNT(*) AS file_count,
       AVG(file_size_in_bytes) / 1024 / 1024 AS avg_size_mb
FROM bronze.ad_clicks.files;

-- 4. 스냅샷 누적 수 확인 (컴팩션 필요 여부)
SELECT COUNT(*) AS snapshot_count
FROM bronze.ad_clicks.snapshots;

-- 5. Silver 중복 이벤트 비율
SELECT COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_count
FROM silver.processed_events
WHERE event_date = current_date;
```

### 운영 대시보드 탭 구성

```
비즈니스 KPI 탭:  CTR / CVR / ROAS 추이
운영 메트릭 탭:   Bronze 신선도 / 이벤트 수 / Iceberg 파일 상태
```

---

## 11. 장애 대응 시나리오

### 11-1. 새벽 Spark Streaming OOM 장애

**상황**: Bronze 스트리밍 잡이 새벽에 OOM(`code 137`)으로 죽음.

**복구 메커니즘**:
- **offset 복구**: `checkpointLocation`(s3a://.../checkpoints/bronze)에 마지막으로 처리한
  Kafka offset이 durable하게 남아 있다. 잡 재시작 시 그 지점부터 정확히 이어 읽는다
  (at-least-once). → [bronze_stream.py](../code/pipelines/bronze_stream.py)의 `checkpointLocation`.
- **중복 방지**: 재시작 과정에서 일부 메시지가 재처리돼 Bronze에 중복 append될 수 있다.
  하지만 Silver가 `event_id` 기준 dedup(ROW_NUMBER latest wins) + MERGE INTO upsert로
  멱등하게 흡수한다. → Bronze는 중복을 "막지" 않고 Silver가 "걸러낸다".
- **메모리 안전벨트**: `maxOffsetsPerTrigger`로 배치당 소비량을 제한해 백로그를 한 번에
  읽다 OOM 나는 것을 방지. 진짜 해결은 EKS executor 오토스케일(KEDA, README 고민 4).
- **raw zone 재처리**: 최악의 경우에도 이미 Bronze(S3)에 적재된 raw는 안전하므로,
  Silver를 raw에서 언제든 재생성할 수 있다.

> ⚠️ 한계(실제 겪음, README 고민 5): Kafka 볼륨 자체가 유실되면 체크포인트가 가리키는
> offset과 Kafka의 실제 offset이 불일치(`failOnDataLoss`)해 미소비 백로그가 영구 손실된다.
> Bronze에 이미 들어온 데이터는 안전. 복구는 체크포인트 리셋 후 현재 Kafka부터 재개.

### 11-2. 정제 로직 버그 발견 → 3개월치 백필

**상황**: Silver 정제 로직에 버그가 있었음을 발견, 과거 3개월치를 다시 처리해야 함.

**대응**:
- **raw 보존이 전제**: Bronze는 변환 없는 raw를 영구 보존하므로 원천이 살아 있다.
  버그 고친 Silver 잡을 `--window-days 90`(또는 기간 지정)으로 재실행하면 된다.
- **MERGE 멱등성**: `event_id` 기준 upsert라 백필이 기존 행을 덮어쓸 뿐 중복을 만들지 않는다.
  몇 번을 돌려도 결과가 같다(WHEN MATCHED UPDATE / NOT MATCHED INSERT).
- **백필 중 대시보드 일관성**: Iceberg는 스냅샷 격리(snapshot isolation)를 제공한다.
  백필 MERGE가 진행 중이어도 대시보드(Athena)는 마지막으로 **커밋된 스냅샷**만 읽으므로
  중간 상태가 노출되지 않는다. 커밋 순간 원자적으로 새 스냅샷으로 전환된다.
- **Expire/Orphan 정책이 백필을 막지 않는가**: 막지 않는다. 백필은 **time-travel이 아니라
  Bronze raw 재처리 + Silver MERGE**로 하므로 Silver 스냅샷 보존기간(retain 7d)과 무관하다.
  단 "Silver를 과거 시점으로 time-travel해서 비교"하려면 그만큼 스냅샷 보존이 필요
  → `iceberg_maintenance.py --snapshot-retention-days`를 백필 윈도우보다 길게 잡으면 된다.
  Bronze raw는 expire가 데이터를 지우는 게 아니라 **구 스냅샷 메타**만 지우므로, 현재
  스냅샷이 가리키는 raw 데이터는 항상 안전하다.

### 11-3. 컴팩션 도중 streaming/batch MERGE가 같은 파티션을 건드림 (OCC)

**상황**: `iceberg_maintenance`의 `rewrite_data_files`(컴팩션)가 도는 동안, Bronze 스트리밍
append나 Silver MERGE가 같은 파티션을 커밋함.

**Iceberg의 감지·중재 (낙관적 동시성, OCC)**:
- 컴팩션은 시작 시점의 스냅샷을 base로 새 데이터 파일을 만든다. commit 시 Iceberg는
  base 스냅샷이 그새 바뀌었는지 검증한다 → 바뀌었으면(다른 write가 먼저 커밋) **충돌을
  감지하고 컴팩션 commit을 실패**시킨다(데이터 손상 없이). 즉 "둘 다 조용히 덮어쓰기"가
  일어나지 않는다.

**운영 회피 패턴 (이 프로젝트 적용)**:
- **시간대 분리**: `iceberg_maintenance` DAG는 `0 4 * * *`(04:00), `silver_processed`는
  `@daily`(자정 기준)으로 스케줄을 분리해 배치 MERGE와 컴팩션이 겹치지 않게 했다.
- **`partial-progress.enabled=true`**: 컴팩션을 여러 commit으로 쪼개, 충돌이 난 파일 그룹만
  실패시키고 나머지는 부분 커밋한다. 실패분은 다음 run에서 처리 → 전체 실패가 아닌 점진 진행.
- **Bronze 스트리밍(상시 write)**: 시간대 분리가 불가하므로 partial-progress에 의존하고,
  `remove_orphan_files`는 `older_than`(72h)으로 in-flight 파일을 보호한다.

> 참고: 더 강한 격리가 필요하면 컴팩션 직전 스트리밍을 잠깐 멈췄다 재개하는 패턴도 있으나,
> 본 프로젝트는 OCC + 시간대 분리 + partial-progress로 충분하다고 판단.

---

## 12. Slack 알림

### 구조

Gold 배치 완료 후 Airflow가 KPI를 체크하고 임계값 초과 시 Slack으로 알림을 전송한다.

```
Gold 적재 완료
    ↓
Airflow: KPI 이상 여부 체크 (Athena 쿼리)
    ↓
임계값 초과 시 → Slack Webhook → 슬랙 채널
```

### 알림 시나리오

| 시나리오 | 조건 | 의심 원인 |
|----------|------|----------|
| CTR 급등 | CTR > 10% | 클릭 어뷰징 (광고 사기) |
| 이벤트 수 급감 | 전 시간 대비 50% 이하 | 파이프라인 장애 |
| Bronze 신선도 저하 | 마지막 적재 > 30분 전 | Spark Streaming 중단 |
| Airflow DAG 실패 | DAG 실패 시 | 배치 처리 오류 |

---

## 13. 100x 스케일 아웃 시나리오 (설계)

일 100만 → 일 1억 이벤트로 성장 시:
