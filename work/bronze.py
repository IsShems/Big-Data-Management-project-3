"""
Batch: Kafka CDC topics -> lakehouse.cdc bronze tables (customers + drivers).

Run via Airflow or: spark-submit --packages ... work/bronze.py
"""
from __future__ import annotations

import os
import sys

import pyspark.sql.functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_spark import new_session

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_PATTERN = os.environ.get(
    "CDC_KAFKA_PATTERN", r"dbserver1\.public\.(customers|drivers)"
)


def run(spark) -> int:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", "earliest")
        .load()
    )
    raw_filtered = raw.filter(F.col("value").isNotNull())
    base = raw_filtered.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("value_str"),
        F.regexp_extract(F.col("topic"), r"dbserver1\.public\.(\w+)$", 1).alias(
            "source_table"
        ),
        F.get_json_object(F.col("value").cast("string"), "$.payload.op").alias("op"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.ts_ms")
        .cast("long")
        .alias("ts_ms"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.source.lsn")
        .cast("long")
        .alias("source_lsn"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after.id")
        .cast("int")
        .alias("after_id"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.before.id")
        .cast("int")
        .alias("before_id"),
    )

    customers_df = base.select(
        "topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "source_table",
        "op",
        "ts_ms",
        "source_lsn",
        "after_id",
        "before_id",
        F.get_json_object(F.col("value_str"), "$.payload.after.name").alias("after_name"),
        F.get_json_object(F.col("value_str"), "$.payload.after.email").alias("after_email"),
        F.get_json_object(F.col("value_str"), "$.payload.after.country").alias("after_country"),
        F.get_json_object(F.col("value_str"), "$.payload.before.name").alias("before_name"),
        F.get_json_object(F.col("value_str"), "$.payload.before.email").alias("before_email"),
        F.get_json_object(F.col("value_str"), "$.payload.before.country").alias("before_country"),
    ).filter(F.col("source_table") == "customers")

    drivers_df = base.select(
        "topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "source_table",
        "op",
        "ts_ms",
        "source_lsn",
        "after_id",
        "before_id",
        F.get_json_object(F.col("value_str"), "$.payload.after.name").alias("after_name"),
        F.get_json_object(F.col("value_str"), "$.payload.after.license_number").alias(
            "after_license_number"
        ),
        F.get_json_object(F.col("value_str"), "$.payload.after.rating")
        .cast("double")
        .alias("after_rating"),
        F.get_json_object(F.col("value_str"), "$.payload.after.city").alias("after_city"),
        F.get_json_object(F.col("value_str"), "$.payload.after.active")
        .cast("boolean")
        .alias("after_active"),
        F.get_json_object(F.col("value_str"), "$.payload.before.name").alias("before_name"),
        F.get_json_object(F.col("value_str"), "$.payload.before.license_number").alias(
            "before_license_number"
        ),
        F.get_json_object(F.col("value_str"), "$.payload.before.rating")
        .cast("double")
        .alias("before_rating"),
        F.get_json_object(F.col("value_str"), "$.payload.before.city").alias("before_city"),
        F.get_json_object(F.col("value_str"), "$.payload.before.active")
        .cast("boolean")
        .alias("before_active"),
    ).filter(F.col("source_table") == "drivers")

    customers_df.writeTo("lakehouse.cdc.bronze_customers").createOrReplace()
    drivers_df.writeTo("lakehouse.cdc.bronze_drivers").createOrReplace()
    return (
        spark.table("lakehouse.cdc.bronze_customers").count()
        + spark.table("lakehouse.cdc.bronze_drivers").count()
    )


def main() -> None:
    spark = new_session("session6_bronze_cdc")
    spark.sparkContext.setLogLevel("WARN")
    try:
        n = run(spark)
        print(f"Bronze CDC rows: {n}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
