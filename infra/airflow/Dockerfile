# 커스텀 Airflow 이미지 — SparkSubmitOperator로 silver 잡을 제출하기 위한 이미지
#
# SparkSubmitOperator는 spark-submit을 Airflow가 직접 호출한다(client 모드 → 드라이버가
# Airflow 안에서 돎). 그래서 Airflow 이미지에 Spark 클라이언트 + Java17이 필요하다.
# 무거운 계산(executor)은 별도 Spark 클러스터(spark-worker)가 수행한다.
#
# python3.11 변형 사용: Spark 3.5는 Python 3.8~3.11 지원 (airflow 기본 3.12와 충돌 회피).

FROM apache/airflow:2.10.5-python3.11

USER root

# Java 17 (Iceberg 1.11 jar는 Java17 컴파일) + procps(spark-submit이 ps 사용)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    # 아키텍처(arm64/amd64) 무관하게 JAVA_HOME 심볼릭 링크
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java17

ENV JAVA_HOME=/opt/java17

# Spark 클라이언트(spark-submit) + silver 잡을 spark 이미지에서 그대로 복사
COPY --from=apache/spark:3.5.3-scala2.12-java17-python3-ubuntu /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${PATH}:/opt/spark/bin"

USER airflow

# Spark provider (SparkSubmitOperator 제공)
RUN pip install --no-cache-dir "apache-airflow-providers-apache-spark==4.11.*"
