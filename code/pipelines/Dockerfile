# Bronze 스트리밍 잡 컨테이너 이미지
#
# 로컬 Spark Structured Streaming으로 Kafka → S3 Iceberg(Bronze) 적재.
# Iceberg + AWS(Glue/S3) + Kafka 커넥터 jar는 spark-submit --packages로 받는다.
# (compose에서 ivy 캐시 볼륨을 마운트해 재다운로드를 방지한다.)

# Iceberg 1.11.0은 Java 17로 컴파일됨 → Java 17 변형 이미지 사용 (기본 3.5.3은 Java 11).
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

# 잡 코드 복사 (이미지 작업 디렉토리는 /opt/spark/work-dir)
COPY bronze_stream.py /opt/spark/work-dir/bronze_stream.py

# 실제 실행 커맨드(spark-submit --packages ...)는 docker-compose.yaml에서 지정한다.
