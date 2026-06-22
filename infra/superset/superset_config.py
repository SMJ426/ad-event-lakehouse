"""Superset 설정 — 로컬 데모용.

PYTHONPATH(/app/pythonpath)에 놓여 Superset이 자동 로드한다.
메타데이터 DB는 postgres(별도 컨테이너), 데이터 소스는 Trino(런타임에 UI/연결로 추가).
"""
import os

# 세션/암호화용 고정 키 (로컬 데모 — 운영 전환 시 교체)
SECRET_KEY = "ad-lakehouse-superset-dev-secret-key-fixed-0123456789abcdef"

# Superset 메타데이터 저장소 (대시보드/차트/사용자 등)
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_METADATA_DB_URI",
    "postgresql+psycopg2://superset:superset@superset-db/superset",
)

# SQL Lab에서 결과를 차트로 만들기 등 편의 기능
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}

# 로컬 데모이므로 CSRF 완화(임포트/탐색 편의). 운영에선 끄지 말 것.
WTF_CSRF_ENABLED = False
