#!/usr/bin/env bash
# kafka-topics.sh — 광고 이벤트 토픽 4개 생성
#
# 두 가지 실행 컨텍스트를 모두 지원한다.
#
#   1) 호스트(맥북)에서 수동 실행:
#        ./infra/kafka-topics.sh
#      → docker exec로 kafka 컨테이너 안의 CLI를 호출 (기본값)
#
#   2) kafka-init 컨테이너 안에서 자동 실행 (docker compose):
#        BOOTSTRAP=kafka:29092 \
#        KAFKA_BIN=/opt/kafka/bin/kafka-topics.sh \
#        ./kafka-topics.sh
#      → CLI를 직접 호출 (컨테이너 안엔 docker가 없으므로)
#
# 토픽 설계:
#   파티션 수: 3 — 50개 캠페인이 3개 파티션에 고르게 분산됨 (campaign_id % 3)
#   복제 인수: 1 — 로컬 단일 브로커 환경. 브로커 수보다 크면 생성 실패.
#              EKS 전환 시 브로커 3개 → 복제 인수를 3으로 올릴 것.
#
# 보존 기간 설계:
#   Kafka는 S3 적재 전 버퍼 역할이다. 보존 기간 = Consumer 장애 허용 시간.
#   광고 어트리뷰션(전환 추적)은 S3/Silver 데이터로 수행 — Kafka 보존과 무관.
#
#   requests / impressions : 72시간  (고볼륨, 유실 영향 낮음)
#   clicks                 : 14일    (저볼륨, 어트리뷰션 키)
#   conversions            : 30일    (극저볼륨, 가장 중요, 업계 전환 윈도우 기준)

set -e  # 명령 실패 시 즉시 종료

# ── 실행 컨텍스트 설정 (환경변수로 오버라이드 가능) ──────────────────────────
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
# kafka-topics CLI를 어떻게 호출할지.
#   호스트:    docker exec kafka /opt/kafka/bin/kafka-topics.sh  (기본값)
#   컨테이너:  /opt/kafka/bin/kafka-topics.sh
KAFKA_BIN="${KAFKA_BIN:-docker exec kafka /opt/kafka/bin/kafka-topics.sh}"

PARTITIONS=3
REPLICATION=1

# 토픽별 보존 기간 (밀리초 단위)
RETENTION_SHORT=$((72  * 60 * 60 * 1000))       #  72시간 (requests, impressions)
RETENTION_MID=$(( 14  * 24 * 60 * 60 * 1000))   #  14일   (clicks)
RETENTION_LONG=$((30  * 24 * 60 * 60 * 1000))   #  30일   (conversions)

echo "=== Kafka 토픽 생성 시작 ==="
echo "브로커: $BOOTSTRAP"
echo "파티션: $PARTITIONS | 복제 인수: $REPLICATION"
echo ""

# Kafka가 준비될 때까지 대기
echo "Kafka 응답 대기 중..."
until $KAFKA_BIN \
  --bootstrap-server "$BOOTSTRAP" \
  --list > /dev/null 2>&1; do
  echo "  아직 준비 중... 3초 후 재시도"
  sleep 3
done
echo "Kafka 준비 완료."
echo ""

# 토픽 생성 함수
create_topic() {
  local TOPIC=$1
  local RETENTION=$2

  if $KAFKA_BIN \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --topic "$TOPIC" > /dev/null 2>&1; then
    echo "[SKIP] $TOPIC 이미 존재함"
  else
    $KAFKA_BIN \
      --bootstrap-server "$BOOTSTRAP" \
      --create \
      --topic "$TOPIC" \
      --partitions "$PARTITIONS" \
      --replication-factor "$REPLICATION" \
      --config retention.ms="$RETENTION"
    echo "[OK]   $TOPIC 생성 완료 (retention=${RETENTION}ms)"
  fi
}

# 토픽 생성 (보존 기간 차등 적용)
create_topic "ad-requests"    "$RETENTION_SHORT"   # 72시간
create_topic "ad-impressions" "$RETENTION_SHORT"   # 72시간
create_topic "ad-clicks"      "$RETENTION_MID"     # 14일
create_topic "ad-conversions" "$RETENTION_LONG"    # 30일

echo ""
echo "=== 생성된 토픽 목록 ==="
$KAFKA_BIN \
  --bootstrap-server "$BOOTSTRAP" \
  --list

echo ""
echo "=== 토픽 상세 정보 ==="
for TOPIC in "ad-requests" "ad-impressions" "ad-clicks" "ad-conversions"; do
  $KAFKA_BIN \
    --bootstrap-server "$BOOTSTRAP" \
    --describe \
    --topic "$TOPIC"
done
