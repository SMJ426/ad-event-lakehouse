"""
criteo_producer.py — Criteo Attribution Dataset 실시간 재생 Producer

역할:
  HuggingFace의 Criteo Attribution Dataset(16.5M건)을 읽어
  Criteo 원본 필드를 변환 없이 Kafka로 전송한다.

Bronze = raw 원칙:
  파생 컬럼을 Producer에서 생성하지 않는다.
  아래 변환은 모두 Silver Glue ETL에서 수행한다.

    cost       → bid_price  (× 1000, CPC → CPM)
    timestamp  → 절대 Unix epoch (_BASE_TIMESTAMP + timestamp)
    cat1       → site_cat   (IAB 카테고리 코드로 매핑)
    cat1+cat2  → banner_id  (파생)
    campaign   → campaign_id (필드명 통일)

이벤트 합성 로직:
  Criteo에는 click / conversion만 존재한다.
  click 1건 기준으로 앞에 있었을 request / impression을 역산하여 합성한다.

    Fill Rate 80%, CTR 2.5% 기준:
      click 1건 → impression 40건, request 50건

  합성 이벤트도 동일한 Criteo 원본 필드를 사용하며,
  cost=0.0 (click 이전 단계, 비용 미발생).

실행:
  python criteo_producer.py               # Kafka로 실제 전송
  DRY_RUN=true python criteo_producer.py  # Kafka 미전송, 이벤트 구조 출력만
"""

import os
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from datasets import load_dataset

import config
from common.schema import CriteoRawEvent, to_topic

# ── DRY_RUN 설정 ───────────────────────────────────────────────────────────────
DRY_RUN: bool = os.environ.get("DRY_RUN", "false").lower() == "true"


# ── 유틸 ───────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _int(row: dict, key: str) -> int:
    """row에서 int 값을 안전하게 추출. None이면 0 반환."""
    val = row.get(key)
    return int(val) if val is not None else 0


# ── 이벤트 생성 ────────────────────────────────────────────────────────────────

def _build_event(event_type: str, row: dict, auction_id: str) -> CriteoRawEvent:
    """
    Criteo 행 하나를 CriteoRawEvent로 변환.

    변환 없이 원본 필드를 그대로 담는다.

    cost 처리:
      request / impression: cost=0.0 (click 전 단계, 비용 미발생)
      click / conversion:   row["cost"] 원본값 그대로
    """
    is_click_or_conv = event_type in ("click", "conversion")

    return CriteoRawEvent(
        event_id=str(uuid.uuid4()),
        event_type=event_type,
        source="criteo",
        auction_id=auction_id,
        produced_at=_now_iso(),

        # Criteo 원본 필드 — 변환 없음
        campaign=int(row["campaign"]),
        uid=str(row["uid"]),
        cost=float(row.get("cost") or 0.0) if is_click_or_conv else 0.0,
        timestamp=int(row["timestamp"]),
        conversion=_int(row, "conversion") if event_type == "conversion" else 0,

        cat1=_int(row, "cat1"),
        cat2=_int(row, "cat2"),
        cat3=_int(row, "cat3"),
        cat4=_int(row, "cat4"),
        cat5=_int(row, "cat5"),
        cat6=_int(row, "cat6"),
        cat7=_int(row, "cat7"),
        cat8=_int(row, "cat8"),
        cat9=_int(row, "cat9"),
    )


# ── Kafka 전송 ─────────────────────────────────────────────────────────────────

def _delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] 전송 실패: {err} | topic={msg.topic()}")


def _produce(producer: Producer, event: CriteoRawEvent) -> None:
    producer.produce(
        topic=to_topic(event),
        key=str(event.campaign).encode("utf-8"),  # 파티션 키: campaign (raw 필드명)
        value=event.to_json_bytes(),
        on_delivery=_delivery_report,
    )


def _emit(producer, event: CriteoRawEvent) -> None:
    if DRY_RUN:
        print(f"[DRY_RUN] topic={to_topic(event)} | {event.to_json_bytes().decode()}")
    else:
        _produce(producer, event)


# ── 메인 루프 ──────────────────────────────────────────────────────────────────

def run() -> None:
    """
    Criteo 데이터셋을 스트리밍으로 읽어 Kafka로 재생한다.

    각 행(click) 처리 순서:
      1. request    × REQUESTS_PER_CLICK(50)건  — 합성, cost=0.0
      2. impression × IMPRESSIONS_PER_CLICK(40)건 — 합성, cost=0.0
      3. click      × 1건  — Criteo 원본 cost 사용
      4. conversion × 1건  — row["conversion"] == 1 인 경우만

    Ctrl+C 로 종료 시 미전송 메시지 flush 후 종료.
    """
    producer = None
    if not DRY_RUN:
        producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
        print(f"[INFO] Kafka 연결: {config.KAFKA_BOOTSTRAP_SERVERS}")

    print(f"[INFO] Criteo 데이터셋 로드 중: {config.CRITEO_DATASET_NAME}")
    print(f"[INFO] Bronze=raw 모드 | 파생 컬럼(banner_id, bid_price 등)은 Silver에서 생성")

    dataset = load_dataset(
        config.CRITEO_DATASET_NAME,
        split="train",
        streaming=True,
    )

    row_count = 0
    total_events = 0

    max_rows = config.CRITEO_MAX_ROWS
    print(f"[INFO] 재생 시작 | DRY_RUN={DRY_RUN} | REPLAY_INTERVAL={config.CRITEO_REPLAY_INTERVAL}s")
    print(f"[INFO] click 1건당: request×{config.REQUESTS_PER_CLICK} + impression×{config.IMPRESSIONS_PER_CLICK} + click×1 (+ conversion 조건부)")
    print(f"[INFO] MAX_ROWS={'무제한' if max_rows <= 0 else f'{max_rows:,}행 후 종료'}")

    try:
        for row in dataset:
            auction_id = str(uuid.uuid4())

            # 1. request × 50건 (합성, cost=0.0)
            for _ in range(config.REQUESTS_PER_CLICK):
                _emit(producer, _build_event("request", row, auction_id))

            # 2. impression × 40건 (합성, cost=0.0)
            for _ in range(config.IMPRESSIONS_PER_CLICK):
                _emit(producer, _build_event("impression", row, auction_id))

            # 3. click × 1건 (Criteo 원본)
            _emit(producer, _build_event("click", row, auction_id))

            # 4. conversion (row["conversion"] == 1 인 경우만)
            if row.get("conversion") == 1:
                _emit(producer, _build_event("conversion", row, auction_id))

            if producer:
                producer.poll(0)

            row_count += 1
            total_events += config.REQUESTS_PER_CLICK + config.IMPRESSIONS_PER_CLICK + 1
            if row.get("conversion") == 1:
                total_events += 1

            if row_count % 10_000 == 0:
                print(f"[INFO] {row_count:,}행 처리 완료 | 총 이벤트 약 {total_events:,}건 발행")

            if max_rows > 0 and row_count >= max_rows:
                print(f"[INFO] MAX_ROWS({max_rows:,}) 도달 — 재생 중단.")
                break

            if config.CRITEO_REPLAY_INTERVAL > 0:
                time.sleep(config.CRITEO_REPLAY_INTERVAL)

    except KeyboardInterrupt:
        print("\n[INFO] 종료 신호 수신. 미전송 메시지 flush 중...")
        if producer:
            producer.flush()

    print(f"[INFO] 재생 완료. 총 {row_count:,}행 처리 | 총 이벤트 약 {total_events:,}건 발행")


if __name__ == "__main__":
    run()
