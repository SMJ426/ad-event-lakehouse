"""
config.py — Producer 공통 설정

역할:
  dummy_producer.py와 criteo_producer.py 양쪽에서 참조하는 설정값 모음.
  값을 바꿀 때 코드를 건드리지 않고 이 파일 하나만 수정하면 된다.

  EKS Kafka 세팅 완료 후 KAFKA_BOOTSTRAP_SERVERS 값을 바꾸는 것이 주요 변경 지점이다.
"""

import os

# ── Kafka 연결 ─────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
# 기본값: kafka:29092 — 같은 Docker 네트워크의 Kafka 서비스 주소.
# compose가 환경변수로 주입한다. EKS 전환 시 NodePort/LoadBalancer 주소로 오버라이드.

# ── Kafka 토픽명 ───────────────────────────────────────────────────────────────
TOPIC_REQUESTS    = "ad-requests"
TOPIC_IMPRESSIONS = "ad-impressions"
TOPIC_CLICKS      = "ad-clicks"
TOPIC_CONVERSIONS = "ad-conversions"

# ── 더미 생성기 속도 ───────────────────────────────────────────────────────────
DUMMY_EPS = 100
# EPS = Events(Auctions) Per Second.
# 100 이면 초당 100개의 request가 발행된다.
# impression/click/conversion은 퍼널 비율에 따라 확률적으로 추가 발생.

DUMMY_MAX_AUCTIONS = int(os.environ.get("DUMMY_MAX_AUCTIONS", "0"))
# 처리할 최대 auction 수. 0 = 무제한(계속 생성).
# 데이터 양을 결정적으로 제어하기 위한 상한 — 이 수만큼 처리 후 자동 종료.
# auction 1건 ≈ request1 + (impression/click/conversion 확률적). [[criteo_max_rows]]와 대칭.

# ── Criteo 재생 속도 ───────────────────────────────────────────────────────────
CRITEO_REPLAY_INTERVAL = float(os.environ.get("CRITEO_REPLAY_INTERVAL", "0.0"))
# 행 하나 처리 후 대기 시간 (초). 환경변수로 오버라이드 (인프라단에서 rate 조절).
# 0.0  = 최대 속도 → Kafka에 폭주, 소비(Spark)가 못 따라가 백로그 폭증.
# 0.05 = 초당 약 20행 → 행당 91이벤트이므로 초당 약 1,800이벤트 (현실적 재생).
# 클수록 느림. 소비속도(Spark maxOffsetsPerTrigger)와 균형을 맞춘다.

CRITEO_MAX_ROWS = int(os.environ.get("CRITEO_MAX_ROWS", "0"))
# 처리할 최대 행(click) 수. 0 = 무제한(데이터셋 끝까지).
# 데이터 양을 결정적으로 제어하기 위한 상한 — 이 수만큼 처리 후 자동 종료.
# 행당 91이벤트(request50+impression40+click1, conversion 조건부)이므로
# 예: 20000행 → 약 182만 이벤트. 로컬 노트북 적정량 수집용.

# ── 광고 퍼널 비율 ─────────────────────────────────────────────────────────────
# 아키텍처 문서 8번 "이벤트 발생 비율" 섹션의 수치와 일치해야 한다.
FILL_RATE = 0.80
# P(impression | request): request가 발생했을 때 impression이 발생할 확률.
# 80% → 요청 10건 중 8건만 실제 광고가 노출됨.

CTR = 0.025
# P(click | impression): impression 발생 시 클릭이 발생할 확률.
# 2.5% → 노출 40건 중 1건 클릭.

CVR = 0.035
# P(conversion | click): click 발생 시 전환(구매)이 발생할 확률.
# 3.5% → 클릭 약 29건 중 1건 전환.

# ── Criteo 역산값 (위 비율로 계산, 수동 변경 금지) ─────────────────────────────
REQUESTS_PER_CLICK    = round(1 / (FILL_RATE * CTR))   # = 50
IMPRESSIONS_PER_CLICK = round(1 / CTR)                  # = 40
# click 1건이 발생하기까지 앞에 있었을 이벤트 수.
# Criteo Producer가 click 1행마다 이 수만큼 합성 이벤트를 앞에 붙여 발행한다.

# ── Criteo 데이터셋 ────────────────────────────────────────────────────────────
CRITEO_DATASET_NAME = "criteo/criteo-attribution-dataset"
# HuggingFace Hub 데이터셋 경로.
# datasets.load_dataset(CRITEO_DATASET_NAME, streaming=True) 로 로드.
