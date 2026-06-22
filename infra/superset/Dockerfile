# Superset + Trino SQLAlchemy 드라이버
# 기본 이미지에 trino dialect만 추가하면 trino:// 연결이 가능해진다.
FROM apache/superset:latest
USER root
# 최신 superset 이미지는 /app/.venv(uv venv)에서 실행되고 system pip은 venv에 설치되지 않는다.
# → uv로 venv python을 직접 대상으로 설치해야 superset이 인식한다.
# trino: 데이터 소스 dialect / psycopg2: Superset 메타DB(postgres) 드라이버(기본 미포함)
RUN uv pip install --python /app/.venv/bin/python --no-cache "trino[sqlalchemy]" psycopg2-binary
USER superset
