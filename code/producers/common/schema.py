"""
schema.py — 이벤트 스키마 정의

Producer별 원칙:
  - dummy_producer   → AdEvent: OpenRTB 형식으로 직접 생성. 파생 컬럼 없음.
  - criteo_producer  → CriteoRawEvent: Criteo 원본 필드를 변환 없이 그대로 담음.

Bronze = raw 원칙:
  Producer는 원본 형식 그대로 Kafka에 넣는다.
  파생 컬럼 생성과 스키마 통일은 Silver Glue ETL의 역할이다.

Silver에서 CriteoRawEvent → OpenRTB 변환 시 할 작업:
  campaign       → campaign_id (필드명 통일)
  cost           → bid_price   (× 1000, CPC → CPM 단위 변환)
  timestamp      → 절대 Unix epoch (_BASE_TIMESTAMP + timestamp)
  cat1           → site_cat    (IAB 카테고리 코드로 매핑)
  cat1 + cat2    → banner_id   (파생)
"""

import json
from dataclasses import dataclass, asdict


@dataclass
class AdEvent:
    """
    dummy_producer 전용 이벤트 스키마.

    더미 이벤트는 처음부터 OpenRTB 형식으로 생성되므로
    모든 필드가 원본이다. 파생 컬럼 없음.
    """

    # ── 이벤트 식별 ───────────────────────────────────────────────────────────
    event_id: str
    # uuid4. Silver에서 중복 제거 시 기준 키.

    event_type: str
    # "request" | "impression" | "click" | "conversion"

    source: str
    # "dummy"

    # ── 경매 묶음 식별 ────────────────────────────────────────────────────────
    auction_id: str
    # uuid4. 같은 경매에서 발생한 request~conversion이 이 값을 공유.
    # Silver에서 퍼널 조인 키로 사용.

    # ── 광고 단위 ─────────────────────────────────────────────────────────────
    campaign_id: int
    # Kafka 파티션 키로도 사용.

    banner_id: str
    banner_w: int
    banner_h: int
    banner_pos: int

    # ── OpenRTB Site ──────────────────────────────────────────────────────────
    site_domain: str
    site_cat: str    # IAB 카테고리 코드. e.g. "IAB1"

    # ── OpenRTB Device ────────────────────────────────────────────────────────
    device_type: int  # 1=mobile, 2=pc, 5=tablet
    os: str
    country: str

    # ── OpenRTB User ──────────────────────────────────────────────────────────
    uid: str

    # ── 광고 비용 ─────────────────────────────────────────────────────────────
    floor_price: float  # 최소 입찰가 (CPM, USD)
    bid_price: float    # 낙찰 금액 (CPM, USD)

    # ── 시각 ──────────────────────────────────────────────────────────────────
    timestamp: int    # 이벤트 발생 시각 (Unix epoch, 초)
    produced_at: str  # Kafka 발행 시각 (ISO 8601 UTC)

    def to_json_bytes(self) -> bytes:
        """Kafka value로 전송할 JSON bytes 반환."""
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")


@dataclass
class CriteoRawEvent:
    """
    criteo_producer 전용 이벤트 스키마.

    Criteo Attribution Dataset의 원본 필드를 변환 없이 그대로 담는다.
    Bronze에 raw 데이터로 저장되며, Silver에서 OpenRTB 스키마로 변환된다.

    request / impression 이벤트는 Criteo에 존재하지 않아 합성한다.
    합성 이벤트도 원본 행의 Criteo 필드를 그대로 사용하며,
    cost=0.0 (click 이전 단계이므로 비용 미발생).
    """

    # ── Producer 생성 필드 (Criteo에 없는 메타 정보) ─────────────────────────
    event_id: str     # uuid4
    event_type: str   # "request" | "impression" | "click" | "conversion"
    source: str       # "criteo"
    auction_id: str   # 퍼널 묶음 ID. Producer가 생성한 uuid4.
    produced_at: str  # Kafka 발행 시각 (ISO 8601 UTC)

    # ── Criteo 원본 필드 (Silver에서 변환) ────────────────────────────────────
    campaign: int
    # Criteo 원본 캠페인 ID.
    # Silver에서 campaign_id로 필드명 통일.
    # Kafka 파티션 키로도 사용 (campaign 단위 집계 최적화).

    uid: str
    # Criteo user ID (int64 → str 변환은 Python 타입 필요로 허용).

    cost: float
    # CPC(클릭당 비용) 단위의 Criteo 원본값.
    # Silver에서 × 1000 하여 CPM 단위 bid_price로 변환.
    # request / impression 합성 이벤트는 0.0.

    timestamp: int
    # 데이터 수집 시작 기준 상대 시간(초). Unix epoch 아님.
    # Silver에서 _BASE_TIMESTAMP + timestamp 로 절대 시각 변환.

    conversion: int
    # 0 | 1. Criteo 원본 전환 여부.
    # request / impression / click 합성 이벤트는 0.

    # Criteo 카테고리 필드 (해시 인코딩된 정수, Silver에서 IAB 코드로 매핑)
    cat1: int   # Silver에서 site_cat(IAB 코드) 파생에 사용
    cat2: int   # Silver에서 banner_id(f"{campaign}_{cat1}_{cat2}") 파생에 사용
    cat3: int
    cat4: int
    cat5: int
    cat6: int
    cat7: int
    cat8: int
    cat9: int

    def to_json_bytes(self) -> bytes:
        """Kafka value로 전송할 JSON bytes 반환."""
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")


def to_topic(event) -> str:
    """
    AdEvent 또는 CriteoRawEvent → Kafka 토픽명 변환.

    두 스키마 모두 event_type 필드를 가지므로 공통 사용 가능.

    예:
      event_type="click"      → "ad-clicks"
      event_type="impression" → "ad-impressions"
    """
    return f"ad-{event.event_type}s"
