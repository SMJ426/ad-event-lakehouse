# 대시보드 — Trino + Superset (Iceberg 위 서빙)

광고 이벤트 레이크하우스의 BI 대시보드. **Spark가 쓴 Iceberg(Gold/Silver/Bronze)를
Trino로 읽어 Superset 대시보드**로 보여준다.

## 아키텍처

```
Superset (BI, :8088)  ──SQL──▶  Trino (쿼리엔진, :8086)  ──▶  Glue Catalog + Iceberg  ──▶  S3
   대시보드/차트                 분산 SQL, 메타테이블 조회         (Spark가 write)
```

> BI 도구는 Iceberg 파일을 직접 고치지 않는다. **SQL 엔진(Trino)에 질의**하고, Trino가
> Glue 카탈로그와 Iceberg metadata를 해석한다. (write=Spark / read=Trino 분리)

**왜 이 조합인가** (강의 7회차 p.26 "팀 BI" 권장 조합):
- **Trino**: Glue 카탈로그 네이티브 + Iceberg 메타테이블(`$files`/`$snapshots`)을 SQL로 조회 →
  비즈니스 KPI뿐 아니라 **운영 메트릭 탭**까지 가능(평가기준 ① 운영 가시성). DuckDB는 메타테이블이 약함.
- **Superset**: 분석가용 BI(SQL Lab + 대시보드), Trino 같은 SQL 엔진 뒤에 두기 적합.

## 실행

```bash
cd infra
docker compose -f docker-compose.dashboard.yaml up -d --build
#   Superset → http://localhost:8088   (admin / admin)
#   Trino    → http://localhost:8086
```

- Superset에 **"Iceberg (Trino)"** 데이터베이스가 이미 등록돼 있다(`trino://admin@trino:8080/iceberg`).
- SQL Lab → DB "Iceberg (Trino)" 선택 → [`trino_queries.sql`](trino_queries.sql)의 쿼리로 차트 생성.

## 대시보드 구성 (두 탭 — 평가 필수)

### 탭 A — 비즈니스 KPI (Gold)
| 차트 | 쿼리 | 내용 |
|---|---|---|
| KPI 빅넘버 | A1 | 총 노출/클릭/전환, cost, CTR/CVR/ROAS |
| 캠페인 성과 Top | A2 | `gold.campaign_daily_stats` cost 랭킹 |
| 시간대 퍼널 | A3 | `gold.hourly_funnel` 24h fill→CTR→CVR |
| 소재 성과 Top | A4 | `gold.banner_daily_stats` CTR + peak_hour |

### 탭 B — 운영 메트릭 (평가기준 ① / 강의 p.19)
| 차트 | 쿼리 | 내용 |
|---|---|---|
| 신선도 | B1 | silver/gold `updated_at`, bronze 최신 스냅샷 |
| 일자별 행 수 | B2 | bronze/silver count by date |
| 중복 상태 | B3 | event_id 중복 수(0 정상) |
| 파일 상태(Small File) | B4 | `$files` file count·avg size |
| 스냅샷 수 | B5 | `$snapshots` count |

## ⚠️ Gold KPI 날짜 기준 (강의 p.16 — README 필수 명시)

본 프로젝트 Gold는 **각 이벤트의 발생일(event_date) 기준** 집계다(attribution 미적용).
- `campaign_daily_stats` / `banner_daily_stats`: event_date(이벤트 발생일)별 집계.
- `hourly_funnel`: event_date × hour.
- conversion을 click_date로 귀속하는 **attribution(예: 14일 윈도우) 기준이 아니다.**
  → 즉 "오늘 전환"은 "오늘 성과"로 집계된다(conversion_date 기준). attribution 리포트는
  matching table(click_conversion_matches) 분리 시 확장 가능(강의 p.13, 추후 과제).
- cost = `SUM(cost) FILTER(event_type='click')` (CPC 모델), ROAS = 전환수 × 가정단가($10) / cost.

## 산출물

- [`trino_queries.sql`](trino_queries.sql) — 차트 뒤 SQL(두 탭 전체).
- `screenshots/` — 비즈니스 KPI 탭 + 운영 메트릭 탭 캡처(발표/제출용).
- (선택) Superset UI에서 대시보드 완성 후 **Settings → Export**로 zip 내보내 이 폴더에 보관하면 재현 가능.

## 종료

```bash
docker compose -f infra/docker-compose.dashboard.yaml down       # 컨테이너 정지
# (메타데이터 보존: superset-pgdata / superset-home 볼륨은 유지됨)
```
