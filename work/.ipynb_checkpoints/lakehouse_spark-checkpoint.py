"""SparkSession with Iceberg REST catalog + MinIO (S3FileIO) — same as interactive cdc_bronze."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def new_session(app_name: str) -> SparkSession:
    rest_uri = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "rest")
        .config("spark.sql.catalog.lakehouse.uri", rest_uri)
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )
