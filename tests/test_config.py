"""
test_config.py — producer 설정 단위 테스트 (Spark/Kafka 불필요, 순수 값)

검사 대상: code/producers/config.py
  - 퍼널 비율(FILL_RATE/CTR/CVR)과 거기서 역산한 합성 비율(REQUESTS_PER_CLICK 등)
  - Kafka 토픽 상수가 to_topic 패턴('ad-{type}s')과 일치하는지

이 값들이 바뀌면 criteo 합성(click 1건당 request/impression 개수)이 어긋나므로, 가정을 고정하는 가드 테스트.
실행: `python -m pytest -q`
"""

import config


def test_funnel_derived_values():
    """click 1건당 합성할 request/impression 수 — FILL_RATE·CTR 가정에서 역산된 값을 고정한다."""
    # REQUESTS_PER_CLICK = round(1 / (FILL_RATE * CTR)) = round(1 / (0.8 * 0.025)) = 50
    assert config.REQUESTS_PER_CLICK == 50
    # IMPRESSIONS_PER_CLICK = round(1 / CTR) = round(1 / 0.025) = 40
    assert config.IMPRESSIONS_PER_CLICK == 40


def test_ratios_in_valid_range():
    """확률(fill_rate/ctr/cvr)은 0 < r <= 1 범위여야 한다 (비율 정의 위반 방지)."""
    for r in (config.FILL_RATE, config.CTR, config.CVR):
        assert 0 < r <= 1


def test_topic_constants_match_pattern():
    """토픽 상수가 schema.to_topic의 'ad-{type}s' 패턴과 일치하는지 (producer↔라우팅 일관성)."""
    assert config.TOPIC_REQUESTS == "ad-requests"
    assert config.TOPIC_IMPRESSIONS == "ad-impressions"
    assert config.TOPIC_CLICKS == "ad-clicks"
    assert config.TOPIC_CONVERSIONS == "ad-conversions"
