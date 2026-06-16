"""
dummy_producer.py — 합성 광고 이벤트 무한 생성 Producer

역할:
  사전에 정의한 Campaign/Banner 풀에서 OpenRTB 형식의 이벤트를 랜덤 조합으로
  생성하여 Kafka로 무한 전송한다. 파이프라인의 주요 볼륨 소스.

실행:
  python dummy_producer.py           # Kafka로 실제 전송
  DRY_RUN=true python dummy_producer.py  # Kafka 미전송, 이벤트 구조 출력만

퍼널(깔때기) 흐름 (1 auction = 1 루프 반복):
  auction_id 생성
    → request 발행 (항상)
    → random() < FILL_RATE  → impression 발행
        → random() < CTR    → click 발행
            → random() < CVR → conversion 발행
  time.sleep(1 / DUMMY_EPS) 후 다음 auction
"""

import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

import config
from common.schema import AdEvent, to_topic

# ── DRY_RUN 설정 ───────────────────────────────────────────────────────────────
# DRY_RUN=True: Kafka 미전송, 이벤트를 stdout에 출력만 한다.
# EKS Kafka가 없는 개발 단계에서 이벤트 구조를 검증할 때 사용.
DRY_RUN: bool = os.environ.get("DRY_RUN", "false").lower() == "true"

# ── 정적 데이터 풀 ─────────────────────────────────────────────────────────────
# 사전 정의된 캠페인 목록 (50개).
# 실무에서는 DB나 외부 설정 파일에서 읽지만, 여기서는 코드에 하드코딩한다.
# campaign_id 분포를 균등하게 유지하여 Kafka 파티션 Hot Spot을 방지한다.
CAMPAIGN_POOL = [
    {"campaign_id": 10000001, "floor_price": 0.30},
    {"campaign_id": 10000002, "floor_price": 0.45},
    {"campaign_id": 10000003, "floor_price": 0.60},
    {"campaign_id": 10000004, "floor_price": 0.50},
    {"campaign_id": 10000005, "floor_price": 0.35},
    {"campaign_id": 10000006, "floor_price": 0.70},
    {"campaign_id": 10000007, "floor_price": 0.40},
    {"campaign_id": 10000008, "floor_price": 0.55},
    {"campaign_id": 10000009, "floor_price": 0.25},
    {"campaign_id": 10000010, "floor_price": 0.80},
    {"campaign_id": 10000011, "floor_price": 0.65},
    {"campaign_id": 10000012, "floor_price": 0.45},
    {"campaign_id": 10000013, "floor_price": 0.30},
    {"campaign_id": 10000014, "floor_price": 0.55},
    {"campaign_id": 10000015, "floor_price": 0.90},
    {"campaign_id": 10000016, "floor_price": 0.35},
    {"campaign_id": 10000017, "floor_price": 0.50},
    {"campaign_id": 10000018, "floor_price": 0.40},
    {"campaign_id": 10000019, "floor_price": 0.75},
    {"campaign_id": 10000020, "floor_price": 0.60},
    {"campaign_id": 10000021, "floor_price": 0.45},
    {"campaign_id": 10000022, "floor_price": 0.30},
    {"campaign_id": 10000023, "floor_price": 0.55},
    {"campaign_id": 10000024, "floor_price": 0.70},
    {"campaign_id": 10000025, "floor_price": 0.40},
    {"campaign_id": 10000026, "floor_price": 0.85},
    {"campaign_id": 10000027, "floor_price": 0.50},
    {"campaign_id": 10000028, "floor_price": 0.35},
    {"campaign_id": 10000029, "floor_price": 0.65},
    {"campaign_id": 10000030, "floor_price": 0.45},
    {"campaign_id": 10000031, "floor_price": 0.30},
    {"campaign_id": 10000032, "floor_price": 0.55},
    {"campaign_id": 10000033, "floor_price": 0.75},
    {"campaign_id": 10000034, "floor_price": 0.40},
    {"campaign_id": 10000035, "floor_price": 0.60},
    {"campaign_id": 10000036, "floor_price": 0.50},
    {"campaign_id": 10000037, "floor_price": 0.45},
    {"campaign_id": 10000038, "floor_price": 0.35},
    {"campaign_id": 10000039, "floor_price": 0.70},
    {"campaign_id": 10000040, "floor_price": 0.80},
    {"campaign_id": 10000041, "floor_price": 0.55},
    {"campaign_id": 10000042, "floor_price": 0.40},
    {"campaign_id": 10000043, "floor_price": 0.65},
    {"campaign_id": 10000044, "floor_price": 0.30},
    {"campaign_id": 10000045, "floor_price": 0.50},
    {"campaign_id": 10000046, "floor_price": 0.45},
    {"campaign_id": 10000047, "floor_price": 0.75},
    {"campaign_id": 10000048, "floor_price": 0.35},
    {"campaign_id": 10000049, "floor_price": 0.60},
    {"campaign_id": 10000050, "floor_price": 0.55},
]

# 캠페인별 배너 목록.
# banner_id 형식: "{campaign_id}_{banner_w}_{banner_pos}"
# 하나의 캠페인은 크기/위치가 다른 배너 소재를 여러 개 운영한다.
BANNER_POOL: dict[int, list[dict]] = {
    c["campaign_id"]: [
        {"banner_id": f"{c['campaign_id']}_300_1", "banner_w": 300, "banner_h": 250, "banner_pos": 1},
        {"banner_id": f"{c['campaign_id']}_728_3", "banner_w": 728, "banner_h": 90,  "banner_pos": 3},
        {"banner_id": f"{c['campaign_id']}_320_1", "banner_w": 320, "banner_h": 50,  "banner_pos": 1},
    ]
    for c in CAMPAIGN_POOL
}

# 광고가 노출될 수 있는 사이트 도메인 목록.
SITE_DOMAINS = [
    "news.example.com",
    "sports.example.com",
    "finance.example.com",
    "tech.example.com",
    "lifestyle.example.com",
    "shopping.example.com",
    "blog.example.com",
    "video.example.com",
]

# IAB 카테고리 코드 (Interactive Advertising Bureau 표준).
SITE_CATS = ["IAB1", "IAB2", "IAB3", "IAB4", "IAB5", "IAB7", "IAB9", "IAB13"]

# device_type → 가능한 OS 매핑.
# OpenRTB 2.6 기준: 1=mobile, 2=pc, 5=tablet
DEVICE_OS_MAP: dict[int, list[str]] = {
    1: ["android", "ios"],
    2: ["windows", "macos"],
    5: ["android", "ios"],
}

# 국가 코드 목록 (한국 비중을 높게 설정).
COUNTRIES = ["KR"] * 7 + ["US", "JP", "SG"]

# ── 이벤트 생성 함수 ────────────────────────────────────────────────────────────

def _now_epoch() -> int:
    """현재 시각을 Unix epoch(초)로 반환."""
    return int(datetime.now(timezone.utc).timestamp())


def _now_iso() -> str:
    """현재 시각을 ISO 8601 UTC 문자열로 반환."""
    return datetime.now(timezone.utc).isoformat()


def _make_event(
    event_type: str,
    auction_id: str,
    campaign: dict,
    banner: dict,
    device_type: int,
    os: str,
    site_domain: str,
    site_cat: str,
    uid: str,
    country: str,
    bid_price: float,
) -> AdEvent:
    """
    공통 AdEvent 객체 생성 헬퍼.
    auction_id를 공유하므로, 한 auction 내 모든 이벤트는 이 값이 동일하다.
    """
    return AdEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        source="dummy",
        auction_id=auction_id,
        campaign_id=campaign["campaign_id"],
        banner_id=banner["banner_id"],
        banner_w=banner["banner_w"],
        banner_h=banner["banner_h"],
        banner_pos=banner["banner_pos"],
        site_domain=site_domain,
        site_cat=site_cat,
        device_type=device_type,
        os=os,
        country=country,
        uid=uid,
        floor_price=campaign["floor_price"],
        bid_price=bid_price,
        timestamp=_now_epoch(),
        produced_at=_now_iso(),
    )


# ── Kafka 전송 ─────────────────────────────────────────────────────────────────

def _delivery_report(err, msg):
    """
    Kafka 비동기 전송 결과 콜백.
    에러 발생 시 로그 출력. 성공 시 무시.
    producer.poll() 호출 시 이 콜백이 실행된다.
    """
    if err is not None:
        print(f"[ERROR] 전송 실패: {err} | topic={msg.topic()}")


def _produce(producer: Producer, event: AdEvent) -> None:
    """
    이벤트를 Kafka 토픽에 발행한다.

    key=str(campaign_id): 같은 캠페인의 이벤트가 같은 파티션에 쌓이도록.
    value=JSON bytes: schema.py의 to_json_bytes() 사용.
    """
    producer.produce(
        topic=to_topic(event),
        key=str(event.campaign_id).encode("utf-8"),
        value=event.to_json_bytes(),
        on_delivery=_delivery_report,
    )


# ── 메인 루프 ──────────────────────────────────────────────────────────────────

def run() -> None:
    """
    광고 이벤트를 무한 생성하여 Kafka로 전송한다.

    DRY_RUN=True 이면 Kafka 연결 없이 이벤트를 stdout에 출력만 한다.
    Ctrl+C 로 종료 시 미전송 메시지를 flush한 뒤 종료한다.
    """
    producer = None
    if not DRY_RUN:
        producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
        print(f"[INFO] Kafka 연결: {config.KAFKA_BOOTSTRAP_SERVERS}")

    interval = 1.0 / config.DUMMY_EPS
    auction_count = 0

    print(f"[INFO] 더미 생성기 시작 | EPS={config.DUMMY_EPS} | DRY_RUN={DRY_RUN}")

    try:
        while True:
            # 1. 이번 auction에 사용할 값들을 랜덤 선택
            campaign    = random.choice(CAMPAIGN_POOL)
            banner      = random.choice(BANNER_POOL[campaign["campaign_id"]])
            device_type = random.choice(list(DEVICE_OS_MAP.keys()))
            os_name     = random.choice(DEVICE_OS_MAP[device_type])
            site_domain = random.choice(SITE_DOMAINS)
            site_cat    = random.choice(SITE_CATS)
            uid         = f"u_{random.randint(1, 1_000_000):07d}"
            country     = random.choice(COUNTRIES)
            # bid_price: floor_price에 20~80% 프리미엄을 붙인 랜덤 낙찰가
            bid_price   = round(campaign["floor_price"] * random.uniform(1.2, 1.8), 4)
            auction_id  = str(uuid.uuid4())

            # 공통 인자 묶음 (event_type만 바꿔서 재사용)
            event_kwargs = dict(
                auction_id=auction_id,
                campaign=campaign,
                banner=banner,
                device_type=device_type,
                os=os_name,
                site_domain=site_domain,
                site_cat=site_cat,
                uid=uid,
                country=country,
                bid_price=bid_price,
            )

            # 2. request: 항상 발행
            req = _make_event("request", **event_kwargs)
            _emit(producer, req)

            # 3. impression: FILL_RATE(80%) 확률로 발행
            if random.random() < config.FILL_RATE:
                imp = _make_event("impression", **event_kwargs)
                _emit(producer, imp)

                # 4. click: CTR(2.5%) 확률로 발행
                if random.random() < config.CTR:
                    clk = _make_event("click", **event_kwargs)
                    _emit(producer, clk)

                    # 5. conversion: CVR(3.5%) 확률로 발행
                    if random.random() < config.CVR:
                        conv = _make_event("conversion", **event_kwargs)
                        _emit(producer, conv)

            # Kafka 비동기 전송 큐 처리 (0 = 블로킹 없이 즉시 반환)
            if producer:
                producer.poll(0)

            auction_count += 1

            # 10,000건마다 진행 상황 출력
            if auction_count % 10_000 == 0:
                print(f"[INFO] auction {auction_count:,}건 처리 완료")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[INFO] 종료 신호 수신. 미전송 메시지 flush 중...")
        if producer:
            producer.flush()
        print(f"[INFO] 종료 완료. 총 {auction_count:,}건 처리.")


def _emit(producer: Producer | None, event: AdEvent) -> None:
    """
    DRY_RUN 여부에 따라 Kafka 전송 또는 stdout 출력을 선택한다.
    """
    if DRY_RUN:
        print(f"[DRY_RUN] topic={to_topic(event)} | {event.to_json_bytes().decode()}")
    else:
        _produce(producer, event)


if __name__ == "__main__":
    run()
