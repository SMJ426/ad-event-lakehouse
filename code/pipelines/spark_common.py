"""
spark_common.py — Spark 잡 공통 설정 (Iceberg Glue Catalog + S3FileIO)

bronze/silver/gold/maintenance 잡이 똑같이 쓰던 환경 상수와 SparkSession 빌더를 한 곳에 모은다.
(이전엔 4개 파일에 build_spark가 거의 동일하게 복붙돼 있었다.)

Dockerfile이 `COPY *.py`로 이 디렉터리를 통째 복사하고, Airflow도 디렉터리를 마운트하므로
각 잡에서 `from spark_common import build_spark, CATALOG, ...`로 바로 쓸 수 있다.
"""

import os

from pyspark.sql import SparkSession

# ── 공통 환경 상수 ────────────────────────────────────────────────────────────
S3_BUCKET = os.environ["S3_BUCKET"]
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
WAREHOUSE = f"s3://{S3_BUCKET}/warehouse"
CATALOG = "glue"


def build_spark(app_name: str, extra_conf: dict | None = None) -> SparkSession:
    """Iceberg Glue Catalog + S3FileIO가 설정된 SparkSession을 생성한다.

    app_name : Spark UI/로그에 표시될 잡 이름.
    extra_conf : 잡별 추가 설정(키→값). 예) 유지보수 잡의 remove_orphan_files용
                 {"spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"}.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        # Iceberg SQL 확장 활성화
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # 'glue' 카탈로그를 Iceberg + AWS Glue Catalog로 연결
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{CATALOG}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{CATALOG}.client.region", AWS_REGION)
        # s3a (스트리밍 체크포인트/고아파일 리스팅용 Hadoop FileSystem) 설정
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
