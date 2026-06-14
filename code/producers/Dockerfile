# Producer 컨테이너 이미지
#
# dummy_generator.py / criteo_producer.py 를 컨테이너로 실행하기 위한 이미지.
# 어떤 Producer를 실행할지는 docker-compose.yaml의 command로 지정한다.

FROM python:3.11-slim

WORKDIR /app

# 의존성 먼저 설치 (레이어 캐시 활용 — 코드만 바뀌면 이 단계 재사용)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY . .

# 기본 실행: 더미 생성기. compose에서 command로 덮어쓸 수 있다.
CMD ["python", "dummy_generator.py"]
