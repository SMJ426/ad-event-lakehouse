# 반영 및 고민 해야할것

> 1. producer 상태에서 데이터 제공이 꼬이면 데이터 관련 멱등성을 지킬 수 있는가 (올바르게 생각한건지 질문)

지금은 producer를 통해 데이터를 생성하고 있지만, 실제 파이프라인에서는 외부 광고 SDK와 같은 외부 소스를 통해 데이터를 쌓게 될것이다.

만약 이러한 외부 소스에 문제가 있어, 데이터가 쌓이다가 똑같은 데이터가 또 쌓이는 상황이 왔을때, (아니면 쌓이다가 말거나)

과연 이 파이프라인에서는 멱등성 문제를 해결 할 수 있을지 의문이 들었다.

다만 고민해봤을때 중복으로 스트리밍 하는건 예외라고 생각하지 않고 정상적으로 언제나 발생할 수 있는 일이라고 생각해보았다.

그렇게 생각하면 "막는다" 기보단 "받아도 걸러낸다"와 같이 접근할 수 있었다.

즉 다른 레이어 (예: Silver)에서 중복을 처리할 수 있을 것 같다. -> 맞나..?

> 2. 단일 브로커에서 데이터생성이 많아진다면 단일 브로커로 감당할 수 있는가? (디스코드 질문 완료)

그렇다고 Docker에서 다중 브로커를 구현한다고 해도 결국은 맥북 docker 죽으면 모든 브로커가 죽지 않나?
그럼 EKS 환경에서 한다고 가정하고 지금은 우선 단일 브로커로 해야하나?
=> 우선은 단일 브로커로 작업하고 추후 EKS로 전환이 쉬운 구조로 감안하고 작업

> 3. Bronze 스트리밍 적재 시 Small File Problem (강의 중 질문 완료)

스트리밍으로 60초마다 작은 배치를 append하니 parquet 파일이 잘게 계속 쌓인다. (배치 N번 → 파일 N개, 1개당 수십 KB - 5차시에 배운 내용에서 보면 이상적인 파일 크기는 128MB~1GB)

파일이 많아지면 읽을 때 오버헤드가 커지고 메타데이터도 비대해진다.

→ Iceberg Compaction(작은 파일 병합) / Expire Snapshots / Orphan Cleanup 으로 해결. 추후 Iceberg 관리 자동화 단계에서 다룰 것.
-> compaction 해주는 배치를 따로 돌리기도 한다.
아무리 실시간이라도 한번에 모아서 붙이기도 한다. (그렇다고 실시간을 해치지 않는 선에서 처리)

> 4. Spark 소비량을 들어오는 양에 따라 유동적으로 가져갈 수 있는가 (디스코드 질문 대기)

지금은 maxOffsetsPerTrigger로 배치당 소비 상한을 고정해뒀다. (상한을 빼면 입력에 비례해 유동적으로 가져가지만, 로컬은 일꾼이 단일 컨테이너라 큰 배치에서 OOM 발생 → 상한이 메모리 안전벨트)

진짜 유동적 소비는 "배치 크기 조절"이 아니라 "consumer(executor) 수를 트래픽에 맞춰 늘리는 것".

→ EKS + Kafka lag 기반 오토스케일(KEDA/HPA)로 해결. 단일 브로커 문제와 동일하게 로컬은 제약 두고 EKS 단계 과제로.

> 5. Kafka 볼륨 유실 시 미소비 데이터 손실 (실습하다가 실제 발생 했던 문제)

Docker 재시작 과정에서 Kafka 메시지가 리셋됐다(4,100만 → 25만). 그런데 Spark 체크포인트는 S3에 살아남아, "offset 115239까지 읽음" vs "Kafka엔 102까지밖에 없음" 불일치 → Spark 크래시(failOnDataLoss).

이게 앞서 4번에서 우려한 "소비가 못 따라가면 미소비 데이터가 사라진다"가 실제로 일어난 사례. Spark가 미처 못 읽은 백로그(~7천만)가 Kafka 볼륨 유실로 영구 손실됨.

다만 **이미 Bronze(S3)에 적재된 데이터는 안전** — Bronze를 영구 저장소로 두는 이유 그 자체. (Kafka는 버퍼일 뿐)

spark 오류로 인해 retension기간 내 소비되지 못한 버퍼의 값들이 발생한다면 엔지니어에게 노티가 가도록 하는것도 방법

→ Kafka 볼륨 durability(EKS는 PVC/복제), ②소비가 생산을 못 따라가면 위험, ③failOnDataLoss는 손실을 "조용히 넘길지 vs 알려줄지" trade-off.
복구는 체크포인트 리셋 후 현재 Kafka부터 재개.

> 6. 슬랙 알림 ?

1. 전주 동요일 대비 가 중요할듯 전주 동요일 대비 지표들이 얼마나 올랐는지, 캡페인별로 슬랙 리포팅이 된다면 문제사항을 미리 감지할 수도 있지 않을까?
   -> 특정 서비스 배포가 나갔는데 그게 광고 지면을 좀더 아래로 내림으로써 광고 노출이 떨어지고 수익이 떨어진다거나 등등.

> 7. 파티션 dt/hour의 타임존 — UTC vs KST

Bronze 파티션 dt/hour는 kafka_timestamp에서 뽑는데 Spark가 UTC 기준으로 계산한다. 즉 hour=5는 UTC 05시 = KST 14시.

광고 분석은 보통 "한국 날짜" 기준으로 보는데, UTC로 파티셔닝하면 KST 자정~오전 9시 데이터가 전날 UTC 파티션에 들어가 "KST 일별 집계"가 두 UTC 파티션에 걸친다.

→ Bronze는 raw라 UTC(Kafka 수신시각) 그대로 두는 게 맞다. KST 변환/파티셔닝을 Silver·Gold에서 할지는 추후 결정.

> 8. Silver dedup 정책(latest wins)과 데이터 오염

Silver dedup은 event_id 중복 시 ingested_at 최신 1건을 남긴다(latest wins). 우리 시뮬레이터에선 중복이 "동일 내용의 재처리"라 어느 쪽을 남겨도 결과가 같다(중복 읽기는 sliding window/재실행에서 의도적으로 발생).

의문: 만약 두 번째로 들어온 데이터가 오염됐다면? → latest wins라 오염된 최신본이 남는 위험이 있다.

→ 정리: dedup은 "같은 걸 두 번 안 세는 것"이지 "옳은 값을 고르는 것"이 아니다. 오염 방어는 dedup이 아니라 **별도 품질 검증 규칙**(cost<0 제거, 범위 이상값 제거 등 — architecture.md 정제 규칙)의 몫. latest wins를 고른 이유는 "나중 도착 = 보정된 정확한 버전"이라는 파이프라인 표준 가정.

**(업데이트) validation 구현 완료** (`silver_processed.py`의 `validate()`): null_event_id / bad_event_type / null_campaign_id / null_uid / negative_cost / timestamp_out_of_range 6규칙으로 무효 행을 적재 전 **drop**하고, 사유별 제거 건수를 잡 로그에 남긴다. validation은 dedup **앞**에 둔다(null event_id가 dedup partitionBy를 오염시키는 것 방지). 처음엔 무효 행을 별도 quarantine 테이블(rejected_events)로 격리하는 방식도 검토했으나, 운영 복잡도 대비 이득이 적어 **drop + 로그 관측**으로 단순화. ⚠️ criteo는 device_type/os/country가 정상 NULL이라 검사 대상에서 제외(공통 키만 null 검사) — 안 그러면 criteo 273만 행이 통째로 걸러진다.

[질문] 이러한 문제도 실무에선 PM이나 경영진의 정책 결정으로 판단하게 되는 것인지?

> 9. Airflow Executor 선택 — LocalExecutor (두뇌+일꾼 겸함)

Silver Airflow를 LocalExecutor로 구성 → scheduler가 스케줄링(두뇌)과 task 실행(일꾼)을 겸한다. 실무는 보통 CeleryExecutor(Redis+Worker 분리)나 KubernetesExecutor(task마다 Pod)로 두뇌와 일꾼을 분리한다. 무거운 task가 scheduler를 마비시키지 않게 하려는 것.

그럼 왜 LocalExecutor? → **우리는 "일꾼 분리"를 Executor 레벨이 아니라 Spark 클러스터 레벨에서 이미 했다.** DAG task는 spark-submit "제출"만 하고(가벼움), 진짜 무거운 계산(dedup/MERGE/수백만 행)은 별도 Spark 클러스터(worker)가 한다. 그러니 Airflow Executor는 방아쇠만 당기면 돼서 scheduler가 겸해도 마비되지 않는다.

추가로 CeleryExecutor는 컨테이너가 6개+로 늘어 디스크 부담(이전 48GB 풀 경험)이 크고, DAG 1개 학습 환경엔 오버엔지니어링.

→ 단일 브로커·체크포인트와 동일 패턴: 로컬은 단순(LocalExecutor), 진짜 분리는 EKS 단계(KubernetesExecutor + Spark on K8s)에서. 단 LocalExecutor는 단일 머신이라 Airflow 자체의 수평 확장은 안 됨 — 우리 경우 task가 가벼워 병목이 아니고, 확장이 필요한 지점은 Spark 클러스터 쪽임.

> 10. Spark 클러스터 단일 worker — 수평 확장과 EKS 대안

지금 Spark는 standalone master 1 + worker 1(4코어/5GB). 무거우면 느려지고(worker 1대 한계) 단일 장애점(SPOF)이다. standalone은 worker 추가가 쉽지만(master 주소만 같으면 자동 등록), **로컬에서 worker를 늘려도 결국 같은 맥북의 CPU·RAM을 나눠 쓰는 것**이라 물리 총량은 그대로 → 진짜 확장이 아니라 흉내. (1 worker 8코어 ≈ 2 worker 4코어, 총량 동일)

진짜 수평 확장 = 여러 물리 노드 필요. EKS(Spark on K8s)가 한 방법이지만 유일하진 않음:
- **매니지드 Spark (가장 자연스러운 대안)**: AWS Glue(서버리스 Spark ETL), EMR/EMR Serverless, Databricks, GCP Dataproc. 클러스터·K8s 관리 없이 진짜 분산 컴퓨팅. 우리는 이미 Glue Catalog+S3+Athena를 쓰므로 **Glue나 EMR Serverless가 최적** — 같은 silver 잡을 거기서 돌리면 됨.
- EC2 여러 대로 Spark standalone, 다른 클라우드 K8s 등도 가능.
→ 즉 self-managed(EKS)냐 managed(Glue/EMR)냐의 선택. 학습·인프라 제어가 목적이면 EKS, 운영 단순·비용 최적이면 매니지드. 로컬 standalone은 어차피 검증용.

[TODO/리팩토링] 로컬에서 처리량 증가는 안 되지만, **worker 수를 늘릴 수 있는 구조를 미리 짜두자**(예: docker compose `--scale spark-worker=N` 또는 worker 서비스 복제 가능하게). 처리량 목적이 아니라 멀티 worker 분산·등록·일 분배 동작을 검증하고, EKS/매니지드 전환 시 노드만 늘리면 되도록 리허설하는 목적. 추후 refactor 단계에서 반영.

> 11. 재수집용 producer 설정(MAX_ROWS / MAX_AUCTIONS / restart 정책)의 상용 함의

**왜 추가했나**: criteo 시뮬레이터가 유한 데이터셋을 무제한 시간 재생하면 수천만 행으로 폭주해 Silver 전량 적재 시 OOM이 났다. 그래서 원천 데이터를 0부터 지우고 **적정량만 재수집**하기로 했고(약 277만 이벤트), 양을 결정적으로 제어하려고 `CRITEO_MAX_ROWS`/`DUMMY_MAX_AUCTIONS`(상한 도달 시 producer 자동 종료) + `restart: on-failure`를 도입했다. 목적은 Gold 대시보드용 깨끗한 데이터 확보.

**상용에서 걸림돌이 되는가** — 기본값이 안전(`0=무제한`)이라 켜지 않으면 상용 동작 그대로다. 다만 둘 다 본질적으로 "시뮬레이터라서 생긴" 설정이다:

- `CRITEO_MAX_ROWS` / `DUMMY_MAX_AUCTIONS`: 애초에 producer가 유한 데이터셋을 재생하는 **시뮬레이터**라 존재하는 knob. 상용에선 producer가 실제 광고 SDK·실시간 이벤트 소스로 대체되며 "최대 행 수" 개념 자체가 사라진다(`CRITEO_REPLAY_INTERVAL`도 동일한 시뮬레이션 throttle). → 상용 전환 시 **제거**되는 개발 전용 설정. 기본 0이라 그대로 둬도 상용 동작은 안 깨짐.
- `restart: always` → `on-failure`: **유일하게 상용 표준에서 벗어난 실제 변경.** 상한 도달 정상 종료(exit 0)를 Docker가 재시작해 재생을 무한 반복하는 걸 막으려 바꿨다. 그런데 24/7 상시 서비스는 보통 `always`/`unless-stopped`가 표준(데몬·호스트 재부팅, 정상 종료에도 자동 복구). `on-failure`도 크래시(비정상 종료) 복구는 되지만 재부팅 자동 기동은 안 된다. → 상용 전환 시 **`unless-stopped`로 환원**해야 한다.

→ **상용 전환 체크리스트**: ① producer를 실제 이벤트 소스로 교체하며 MAX_ROWS/MAX_AUCTIONS/REPLAY_INTERVAL 제거, ② producer `restart` 정책을 `unless-stopped`로 환원. 정리하면 이 설정들은 "조용히 깨지진 않지만 상용으로 그대로 들고 가면 안 되는" 로컬·시뮬레이션 전용 장치다.

> 12. Iceberg 매니지먼트 자동화 — 적재와 유지보수의 분리

Small File 문제(Bronze 60초 마이크로배치가 테이블당 작은 parquet 양산)와 스냅샷·매니페스트 무한 누적을 해결하기 위해 **compaction / expire_snapshots / remove_orphan_files**를 Airflow DAG(`iceberg_maintenance`)로 자동화했다(`code/pipelines/iceberg_maintenance.py`). 실측: Bronze 4테이블 각 19개 작은 파일(평균 2.5MB) → 컴팩션 후 1개(47.5MB), 스냅샷 20→5(만료).

**설계 결정**:
- **적재 ≠ 유지보수 분리**: 유지보수를 적재 잡(bronze_stream/silver_processed)과 별도 잡·별도 DAG로 뒀다. 유지보수가 실패해도 적재 파이프라인은 영향이 없다.
- **순서 = compaction → expire → orphan**: 컴팩션이 새 스냅샷(operation=replace)을 만든 뒤, expire가 구 스냅샷을, orphan이 어디서도 참조 안 되는 파일을 회수한다. 순서가 거꾸로면 회수할 대상이 아직 안 생긴다.
- **OCC(낙관적 동시성) 대응**: `rewrite_data_files`는 commit 시 base 스냅샷이 바뀌면 충돌을 감지해 실패시킨다(손상 없음). 회피책으로 ① `iceberg_maintenance`(04:00)와 `silver_processed`(자정) **시간대 분리**, ② `partial-progress.enabled`로 충돌 그룹만 실패·나머지 부분 커밋. (architecture §11-3)

**구현 중 겪은 실제 이슈 2가지**:
- `remove_orphan_files`는 `older_than < 24h`를 **Iceberg가 차단**한다(동시 작업 중 in-flight 파일 손상 방지). → 잡에서 24h 미만은 24h로 클램프 + 경고.
- orphan은 테이블 위치(`s3://`)를 **Hadoop FileSystem으로 직접 리스팅**하는데, 적재 잡은 `s3a` 체크포인트만 써서 클러스터에 `hadoop-aws`가 없었고 `s3` 스킴 매핑도 없었다. → `fs.s3.impl=S3AFileSystem` 설정 + `hadoop-aws`/`aws-java-sdk-bundle` jar를 `--jars`에 추가(S3FileIO만 쓰는 compaction/expire와 달리 orphan만의 추가 의존성).

> 13. Gold 레이어 — 대시보드 계약, cost 정의, 증분 처리

Gold(`gold_aggregations.py`)는 Silver(이벤트 단위)를 KPI로 집계한 서빙용 3테이블(campaign_daily_stats / banner_daily_stats / hourly_funnel)이다. silver DAG 하류(`silver_processed_merge >> gold_aggregate`)로 자동 실행. 실측: campaign 669 / banner 4006 / hourly 7행.

**설계 결정 3가지**:
- **대시보드에서 거꾸로 설계**: "Gold를 잘 만들려면 대시보드를 먼저 정해야 하나?" → 그렇다. 각 Gold 테이블이 어떤 대시보드 뷰를 서빙하는지 매핑(계약)을 먼저 고정하고 그에 맞춰 만들었다. criteo는 한 날에 몰려 **시간대 분포(hourly_funnel)**, dummy는 매일 누적되니 **일별 추세(campaign_daily)** — 데이터원을 강점에 맞게 분담.
- **cost = `SUM(cost) FILTER(event_type='click')`**: criteo는 click+conversion, dummy는 전 이벤트가 cost를 가져 단순 SUM 시 중복/폭증한다. CPC 모델로 통일. (검증: Gold cost 7858.04 == Silver click cost 7858.04로 일치) ROAS는 매출이 없어 전환당 가정 단가(상수)로 계산.
- **증분 = updated_at 기준 (event_date 아님)**: "지금은 작은 데이터지만 실제론 날짜별로 누적된다"는 점을 반영해 전량 재계산이 아니라 증분으로 설계. 그런데 event_date(이벤트 발생시각)로 윈도우를 잡으면 criteo(2024)가 누락된다 → **updated_at(Silver 처리시각)으로 최근 변경분의 event_date를 골라** 그 파티션만 `overwritePartitions`. late data도 자동 반영, 매일 바뀐 날짜만 갱신.

**검증으로 잡은 것**: 퍼널 단조성(req≥imp≥click≥conv) 위반 0, 비율 범위(0~1) 위반 0, hourly_funnel에서 criteo가 fill 0.8·ctr 0.025로 **합성 비율(80%·2.5%) 그대로** 재현됨을 확인. (그리고 "dummy 캠페인 50개"가 버그인 줄 알았으나 풀이 실제 50개여서 정상 — 출력을 직접 보고 가정을 검증한 사례)

> 14. Silver MERGE를 조건부로 — sliding window × 무조건 UPDATE의 증분 낭비

**발견한 문제**: Silver는 매일 7일 sliding window로 Bronze를 다시 읽어 MERGE한다. 그런데 기존 MERGE가 `WHEN MATCHED THEN UPDATE SET *`(무조건 UPDATE)라서, **내용이 안 바뀐 행도 매일 `updated_at`이 갱신**됐다. 그러면 Gold(고민 13, `updated_at` 기준 증분)가 **실제로 안 바뀐 파티션도 매일 재집계**한다. 특히 우리 데이터는 criteo 273만 행이 최근 dt라 매일 재-MERGE → 2024-01-01 파티션 전체를 매일 헛계산. **틀리진 않지만(멱등) 낭비**다.

**해결**: MERGE를 조건부로 — *비즈니스 컬럼이 실제로 다를 때만* UPDATE.
```sql
WHEN MATCHED AND NOT (t.col1 <=> s.col1 AND t.col2 <=> s.col2 AND ...) THEN UPDATE SET *
```
- `<=>`(null-safe 동등)을 써서 criteo의 정상 NULL(device/os/country)도 안전 비교(`<>`는 NULL이면 변경을 놓침).
- 비교 대상 = `FINAL_COLS` − {event_id(키), updated_at(메타)}.
- 효과: 안 바뀐 행은 UPDATE 스킵 → `updated_at` 유지 → Gold가 그 날짜를 "안 바뀐 날"로 보고 건너뜀 → **증분이 진짜 증분이 됨**. (검증: Silver 재실행해도 max(updated_at)이 14:03:37로 불변)

**검증으로 잡은 중요한 한계 (COW의 성질)**: 조건부로 바꿔도 **Iceberg COW MERGE는 ON 조건(event_id)에 매칭되는 데이터 파일을 통째로 다시 쓴다.** 7일 윈도우 source가 모든 event_id를 덮으니 매 실행마다 매칭 파일 전부가 rewrite된다(스냅샷: overwrite, added=deleted=전체 행수). 단 `WHEN MATCHED AND NOT(...)`이 false라 **행은 옛 값(옛 updated_at) 그대로 되써질 뿐**이다.
→ 즉 이 변경은 **"updated_at 보존(= Gold 증분 정상화)"이 목적이고 그건 달성**했지만, **파일 rewrite(=compaction/스토리지 churn)는 줄지 않는다.** 파일 churn까지 없애려면 MERGE source를 실제 신규/변경분으로 좁히거나(anti-join), MOR(merge-on-read)로 전환해야 함 — 별도 과제. 처음엔 "compaction 부담도 준다"고 적었으나 스냅샷을 보고 정정(출력 검증으로 잡은 오판).

**남은 트레이드오프**: late-arriving 전환(conversion_delay_sec 변경)은 비교 대상에 포함돼 정상 반영. sliding window 자체(7일 재읽기)는 늦게 오는 데이터 대비로 유지. 윈도우 폭/Gold lookback은 별도 튜닝 레버.

> 15. cost 모델링의 모호함 — bid_price를 cost로 매핑한 것

**의문**: Gold에서 광고비를 집계하다 보니 `cost`가 모든 이벤트(request/impression/click/conversion) 행에 같은 값으로 들어있었다. 왜?

**원인**: `bid_price`(낙찰가)는 원래 **경매(auction) 단위 속성**이다. 그런데 dummy producer(`_make_event`)가 한 경매의 맥락(bid_price 등)을 그 경매에서 파생된 **모든 이벤트 행에 복사**해 넣는다(각 이벤트가 자기완결로 맥락을 들고 다님 — raw 이벤트 로그에선 정상). 그리고 Silver가 `bid_price → cost`로 이름을 바꾸면서, "cost(비용)"라는 이름 탓에 **모든 이벤트가 광고주 비용인 것처럼 보였다.** 실제로는 같은 경매 가격의 반복일 뿐, 4번 과금이 아니다.

**반영(해결)**: 실제 과금은 **과금 모델이 정하는 한 시점**에만 일어난다. 본 프로젝트는 **CPC(Cost Per Click, 클릭당 과금)** 로 가정 → Gold cost = `SUM(cost) FILTER(event_type='click')` 로 **click 행의 cost만** 합산해 중복 합산을 피한다(고민 13 / `_click_cost`). 검증: Gold cost == Silver click cost 합(7858.04)으로 일치.

**남은 개선점**: "경매 가격(bid_price)"과 "실제 과금액(cost)"을 별도 컬럼으로 구분하거나, 과금 이벤트(click)에만 cost를 두면 모델이 더 명확해진다. 현재는 bid_price를 cost로 복사 + Gold에서 click만 합산해 **결과는 맞게** 처리. (지금은 CPC 단일 가정 — 모델이 섞이면 과금 모델 필드를 두고 모델별 과금 이벤트를 골라 합산하도록 확장)

# 광고 이벤트 레이크하우스

## 1. 도메인 정의 + 핵심 KPI 3개

## 2. 전체 아키텍처 (그림 + 설명)

## 3. 메달리온 3계층 의사결정

- 3-1. Bronze (raw)
- 3-2. Silver (processed)
- 3-3. Gold (summary)

## 4. 이 도메인에서 Iceberg가 가장 가치 있는 지점

## 5. 운영 헬스 체크 쿼리 모음

## 6. 대시보드 (스크린샷 + 운영 메트릭)

## 7. 100x 스케일 아웃 시나리오 (설계만, 구현 X)

## 8. 장애·운영 시나리오

## 9. 멱등성 / 재처리 가능성 설계
