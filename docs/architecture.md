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

### 3-1. 웹 이벤트 (실시간)
> open rtb(실제 광고업계 표준 값 참고해서 실제 처럼 구상하면 좋음) - 더미데이터 (참고)
> https://iabtechlab.com/wp-content/uploads/2022/04/OpenRTB-2-6_FINAL.pdf


자체 구축한 광고 배너 웹페이지에서 발생하는 진짜 실시간 이벤트.

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

### 3-2. Criteo Attribution Dataset (시뮬레이션)

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

**공통 이벤트 스키마: - 추후 개발하면서 변경될 수 있음** - 실무 

```json
{
  "event_id":    "uuid4",
  "event_type":  "click",
  "source":      "web | criteo",
  "banner_id":   "CAMP_042_A_X",
  "campaign_id": "CAMP_042",
  "uid":         "user_abc123",
  "timestamp":   1456790400,
  "produced_at": "2024-01-15T10:00:00Z"
}
```

> `source` 필드로 웹 이벤트와 Criteo 재생 이벤트를 구분.


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

## 9. 장애 대응 시나리오

### 새벽 OOM 장애

### 정제 로직 버그 발견 (백필)


---

## 10. Slack 알림

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

## 11. 100x 스케일 아웃 시나리오 (설계)

일 100만 → 일 1억 이벤트로 성장 시:
