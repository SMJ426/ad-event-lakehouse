#!/usr/bin/env bash
# aws-setup.sh — Bronze 레이어용 AWS 리소스 프로비저닝
#
# 생성하는 것:
#   1. S3 버킷            ad-events-lakehouse-<accountid>
#        ├─ warehouse/       Iceberg 데이터/메타데이터
#        └─ checkpoints/     Spark Structured Streaming 체크포인트
#   2. Glue 데이터베이스   bronze
#
# Athena는 Bronze 적재의 일부가 아니라 조회/서빙 도구이므로 여기서 설정하지 않는다.
# Bronze 검증은 `aws s3 ls` + Spark count로 한다. (Athena는 이후 서빙 단계에서 설정)
#
# 사전 조건:
#   aws configure 완료 (credentials + region 설정).
#
# 실행:
#   chmod +x infra/aws-setup.sh
#   ./infra/aws-setup.sh
#
# 멱등성: 이미 존재하는 리소스는 SKIP. 여러 번 실행해도 안전하다.

set -euo pipefail

REGION="ap-northeast-2"
GLUE_DB="bronze"

echo "=== AWS 리소스 프로비저닝 시작 (region=$REGION) ==="

# ── 0. 자격증명 확인 + account id 취득 ──────────────────────────────────────
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
echo "AWS 계정: $ACCOUNT_ID"

BUCKET="ad-events-lakehouse-${ACCOUNT_ID}"
echo "버킷명: $BUCKET"
echo ""

# ── 1. S3 버킷 생성 ─────────────────────────────────────────────────────────
if aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "[SKIP] S3 버킷 $BUCKET 이미 존재함"
else
  aws s3api create-bucket \
    --bucket "$BUCKET" \
    --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION" > /dev/null
  echo "[OK]   S3 버킷 $BUCKET 생성 완료"
fi

# 프리픽스는 객체가 생기면 자동 생성되므로 빈 디렉토리 마커만 안내 목적으로 둔다.
for prefix in warehouse checkpoints; do
  aws s3api put-object --bucket "$BUCKET" --key "${prefix}/" > /dev/null 2>&1 || true
done
echo "       프리픽스 준비: warehouse/, checkpoints/"
echo ""

# ── 2. Glue 데이터베이스 생성 ───────────────────────────────────────────────
if aws glue get-database --name "$GLUE_DB" --region "$REGION" > /dev/null 2>&1; then
  echo "[SKIP] Glue 데이터베이스 '$GLUE_DB' 이미 존재함"
else
  aws glue create-database \
    --region "$REGION" \
    --database-input "Name=${GLUE_DB},Description=Bronze raw layer for ad events" > /dev/null
  echo "[OK]   Glue 데이터베이스 '$GLUE_DB' 생성 완료"
fi
echo ""

# ── 완료 ────────────────────────────────────────────────────────────────────
echo "=== 프로비저닝 완료 ==="
echo ""
echo "다음 단계에서 이 버킷명을 사용한다:"
echo ""
echo "    S3_BUCKET=$BUCKET"
echo ""
echo "infra/.env 파일에 기록하려면:"
echo "    echo \"S3_BUCKET=$BUCKET\" > infra/.env"
