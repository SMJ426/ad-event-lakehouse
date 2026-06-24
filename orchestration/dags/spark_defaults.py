"""
spark_defaults.py — 여러 DAG가 공통으로 쓰는 Spark jar/conf 상수.

(DAG 객체가 없으므로 스케줄러는 이 파일을 잡으로 등록하지 않는다. 같은 폴더라
 다른 DAG 파일에서 `from spark_defaults import ...`로 import만 한다.)

jar는 ivy-cache 볼륨(/opt/cache)에 미리 캐시된 것을 직접 지정해 --packages 런타임
다운로드(충돌·실패)를 피한다.
"""

_JAR_DIR = "/opt/cache/jars"

# 적재/압축 잡용: Iceberg + AWS 번들(S3FileIO). 대부분의 잡은 이걸로 충분.
ICEBERG_JARS = (
    f"{_JAR_DIR}/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.11.0.jar,"
    f"{_JAR_DIR}/org.apache.iceberg_iceberg-aws-bundle-1.11.0.jar"
)

# 유지보수(remove_orphan_files)용: 위 + hadoop-aws(S3A) + aws-java-sdk.
# 고아 파일 탐색이 s3:// 위치를 Hadoop FileSystem으로 리스팅하므로 추가 의존성이 필요하다.
ICEBERG_JARS_WITH_HADOOP = (
    ICEBERG_JARS + ","
    f"{_JAR_DIR}/org.apache.hadoop_hadoop-aws-3.3.4.jar,"
    f"{_JAR_DIR}/com.amazonaws_aws-java-sdk-bundle-1.12.262.jar"
)

# 단일 worker 로컬 환경 기준 공통 메모리 설정.
SPARK_CONF = {"spark.driver.memory": "2g", "spark.executor.memory": "4g"}
