"""
test_schema.py — 이벤트 스키마 단위 테스트 (Spark/Kafka 불필요, 순수 로직)

검사 대상: code/producers/common/schema.py
  - to_topic(event)         : event_type → Kafka 토픽명 매핑(이벤트 라우팅의 핵심)
  - <event>.to_json_bytes() : dataclass → Kafka로 보낼 JSON bytes 직렬화

단위 테스트 = "작은 함수 하나가 의도대로 동작하는지" 자동 검증. assert가 거짓이면 그 테스트는 실패한다.
실행: `python -m pytest -q`
"""

import json

import pytest

from common.schema import AdEvent, CriteoRawEvent, to_topic


def _sample_ad_event(event_type: str = "click") -> AdEvent:
    """테스트용 최소 AdEvent (모든 필드가 필수라 헬퍼로 한 번에 구성)."""
    return AdEvent(
        event_id="e1", event_type=event_type, source="dummy", auction_id="a1",
        campaign_id=10000001, banner_id="10000001_300_1", banner_w=300, banner_h=250, banner_pos=1,
        site_domain="news.example.com", site_cat="IAB1", device_type=1, os="android",
        country="KR", uid="u_0000001", floor_price=0.3, bid_price=0.45,
        timestamp=1700000000, produced_at="2026-01-01T00:00:00+00:00",
    )


# ── to_topic: event_type → 토픽명 ───────────────────────────────────────────────
@pytest.mark.parametrize("event_type, expected", [
    ("request", "ad-requests"),
    ("impression", "ad-impressions"),
    ("click", "ad-clicks"),
    ("conversion", "ad-conversions"),
])
def test_to_topic_maps_event_type(event_type, expected):
    """event_type이 올바른 Kafka 토픽으로 매핑되는지 — 잘못되면 메시지가 엉뚱한 토픽으로 간다."""
    assert to_topic(_sample_ad_event(event_type)) == expected


def test_to_topic_works_for_criteo_event():
    """to_topic은 두 스키마 공통(event_type만 봄) → CriteoRawEvent도 동일하게 동작해야."""
    ev = CriteoRawEvent(
        event_id="e1", event_type="click", source="criteo", auction_id="a1",
        produced_at="2026-01-01T00:00:00+00:00", campaign=7, uid="42", cost=0.1,
        timestamp=10, conversion=0, cat1=1, cat2=2, cat3=0, cat4=0, cat5=0,
        cat6=0, cat7=0, cat8=0, cat9=0,
    )
    assert to_topic(ev) == "ad-clicks"


# ── to_json_bytes: 직렬화 라운드트립 ────────────────────────────────────────────
def test_to_json_bytes_roundtrip():
    """to_json_bytes() → JSON 파싱 시 필드가 보존되는지 (Kafka로 보내는 payload 무결성)."""
    ev = _sample_ad_event("impression")
    decoded = json.loads(ev.to_json_bytes().decode("utf-8"))
    assert decoded["event_type"] == "impression"
    assert decoded["source"] == "dummy"
    assert decoded["campaign_id"] == 10000001
    assert decoded["bid_price"] == 0.45


def test_to_json_bytes_returns_bytes():
    """반환 타입은 bytes여야 한다 (Kafka value 요구사항)."""
    assert isinstance(_sample_ad_event().to_json_bytes(), bytes)
