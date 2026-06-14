"""
config.py — Producer 공통 설정

역할:
  dummy_generator.py와 criteo_producer.py 양쪽에서 참조하는 설정값 모음.
  값을 바꿀 때 코드를 건드리지 않고 이 파일 하나만 수정하면 된다.

  EKS Kafka 세팅 완료 후 KAFKA_BOOTSTRAP_SERVERS 값을 바꾸는 것이 주요 변경 지점이다.
"""

import os

# ── Kafka 연결 ─────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
# 기본값: localhost:9092 — 호스트(맥북)에서 직접 python 실행할 때.
# 컨테이너로 실행 시 환경변수로 오버라이드: KAFKA_BOOTSTRAP_SERVERS=kafka:29092
#   (같은 Docker 네트워크 안에서는 서비스 이름 kafka로 접속)
# EKS 전환 시: NodePort 또는 LoadBalancer 주소로 오버라이드.
#   예) "a1b2c3d4.ap-northeast-2.elb.amazonaws.com:9092"

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

# ── Criteo 재생 속도 ───────────────────────────────────────────────────────────
CRITEO_REPLAY_INTERVAL = 0.0
# 행 하나 처리 후 대기 시간 (초).
# 0.0 = 최대 속도 (16.5M건을 가능한 빠르게 재생).
# 0.001 = 초당 약 1,000행 처리.
# 실시간처럼 보이게 하고 싶으면 값을 높인다.

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
