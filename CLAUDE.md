# CLAUDE.md

Claude/AI 어시스턴트가 이 레포에서 작업할 때 **먼저 읽는 안내서**. (사람이 전체 그림을 빠르게 잡는 용도로도 OK.)

## 프로젝트
광고 이벤트 **레이크하우스**. Criteo Attribution Dataset 재생 + 합성 더미 이벤트를 Kafka로 흘려보내고,
Spark + Apache **Iceberg**(Glue 카탈로그 / S3)로 **메달리온(Bronze→Silver→Gold)** 파이프라인을 돌린 뒤,
**Trino+Superset**으로 서빙, **Airflow**로 오케스트레이션, **Slack**으로 운영 알림/리포트.

## 아키텍처 (데이터 흐름)
```
producers(Kafka) ─▶ Spark Structured Streaming ─▶ Bronze (Iceberg/S3, raw)
                                                       │ Spark batch(MERGE)
                                                       ▼
                                                  Silver (정제·dedup by event_id)
                                                       │ Spark batch(집계)
                                                       ▼
                                                  Gold (KPI: campaign/banner/hourly)
                                                       │
                               Trino ─▶ Superset (BI 대시보드 + 운영 메트릭 탭)
오케스트레이션: Airflow DAGs   ·   알림/리포트: Slack (webhook 경보 + bot 리포트)
```
- **write=Spark / read=Trino** 분리. Bronze=raw(파싱 X), Silver=파싱·스키마 통일·dedup, Gold=KPI 집계.

## 레포 구조
- `code/pipelines/` — Spark 잡: `bronze_stream.py`(적재 스트리밍), `silver_processed.py`(MERGE 정제),
  `gold_aggregations.py`(KPI), `iceberg_maintenance.py`(compaction/expire/orphan),
  `pipeline_health.py`(헬스체크), `daily_report.py`(KPI 리포트), `seed_demo_gold.py`(데모 합성 백필),
  `spark_common.py`(공통 `build_spark`/상수).
- `code/producers/` — Kafka producer: `dummy_producer.py`, `criteo_producer.py`, `config.py`,
  `common/schema.py`(이벤트 스키마).
- `code/ddl/`, `code/health-queries/` — 참조 SQL.
- `orchestration/dags/` — Airflow DAG: `medallion_dag.py`, `iceberg_maintenance_dag.py`,
  `health_check_dag.py`, `daily_report_dag.py` + 공통 모듈 `spark_defaults.py`(jar/conf), `slack_alert.py`(실패 콜백).
- `infra/` — `docker-compose.yaml`(Bronze: kafka+producers+spark-bronze),
  `docker-compose.airflow.yaml`(airflow+spark 클러스터), `docker-compose.dashboard.yaml`(trino+superset),
  `airflow/`·`trino/`·`superset/`(이미지/설정), `.env`(비밀, gitignore).
- `dashboard/` — Trino 쿼리 + Superset 운영 탭 import 번들. `docs/` — architecture/가이드. `tests/` — 단위 테스트.

## 실행 (스택 3개, compose 별도)
```bash
cd infra
docker compose -f docker-compose.yaml up -d            # ① Bronze 적재 (Kafka → S3)
docker compose -f docker-compose.airflow.yaml up -d    # ② Silver/Gold/health/report — Airflow UI :8082 (admin/admin)
docker compose -f docker-compose.dashboard.yaml up -d  # ③ 대시보드 — Superset :8088 (admin/admin)
```
- **필수 env**: `infra/.env`에 `S3_BUCKET=...` (+ 선택 `SLACK_WEBHOOK_URL`/`SLACK_BOT_TOKEN`/
  `SLACK_REPORT_CHANNEL`/`APP_ENV`). AWS 자격증명은 `~/.aws` 마운트(코드에 키 하드코딩 X).
- **DAG**: `medallion`(15분, Silver+Gold+compaction) · `iceberg_maintenance`(daily 04:00 GC) ·
  `health_check`(매시간) · `daily_report`(daily 09 KST). 새 DAG는 기본 paused → `airflow dags unpause <id>`.
- **Spark 잡 수동 실행**(예: 헬스체크):
```bash
docker exec airflow-scheduler /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --jars /opt/cache/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar,/opt/cache/jars/org.apache.iceberg_iceberg-aws-bundle-1.11.0.jar \
  /opt/spark/work-dir/pipeline_health.py
```

## 컨벤션
- Spark 잡: `from spark_common import build_spark, CATALOG` 재사용. `S3_BUCKET`/`AWS_REGION` env 필수.
  카탈로그명 `glue`, warehouse `s3://$S3_BUCKET/warehouse`.
- DAG: jar/conf는 `spark_defaults`(`ICEBERG_JARS` / `ICEBERG_JARS_WITH_HADOOP` / `SPARK_CONF`),
  실패 알림은 `slack_alert.slack_failure_callback`(default_args의 `on_failure_callback`).
- 비밀은 `infra/.env`(gitignore). 코드/주석/문서는 **한국어**.
- producers는 **컨테이너 전용**(호스트 직접 실행 로직 없음). 종료 시 `try/finally`로 flush 보장.

## 주의 (gotcha)
- **jar 경로가 환경마다 다름**: airflow 컨테이너=`/opt/cache/jars/`, spark-master=`/opt/spark/.ivy2/jars/`.
- **로컬 메모리**: 세 스택을 동시에 다 띄우면 JVM(Spark/Trino) 메모리 압박으로 **trino가 OOM(exit 137)** 날 수 있음 →
  안 쓰는 스택은 내리거나 DAG pause.
- **데모 합성 데이터**: `seed_demo_gold.py`로 넣은 gold 합성 행은 `seed_demo_gold.py --clean`으로 제거.

## 테스트
Spark 없이 도는 순수 로직 단위 테스트가 `tests/`에 있다:
```bash
pip install -r requirements-dev.txt
python -m pytest -q
```
- 대상: `code/producers/common/schema.py`(토픽 매핑·JSON 직렬화), `code/producers/config.py`(퍼널 역산값/비율).
- `push`/`pull_request`마다 GitHub Actions(`.github/workflows/ci.yml`)가 자동 실행.
- (Spark 잡 통합 테스트는 SparkSession 필요 → CI 무거워져서 향후 과제.)
