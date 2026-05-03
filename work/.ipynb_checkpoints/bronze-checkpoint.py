"""
Batch: Kafka CDC topic -> lakehouse.cdc.bronze_customers (Iceberg, full refresh).

Run via Airflow or: spark-submit --packages ... work/bronze.py
"""
from __future__ import annotations

import os
import sys

import pyspark.sql.functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_spark import new_session

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("CDC_KAFKA_TOPIC", "dbserver1.public.customers")


def run(spark) -> int:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )
    raw_filtered = raw.filter(F.col("value").isNotNull())
    bronze_df = raw_filtered.select(
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.op").alias("op"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.ts_ms").cast("long").alias("ts_ms"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after.id").cast("int").alias("after_id"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after.name").alias("after_name"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after.email").alias("after_email"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.after.country").alias("after_country"),
        F.get_json_object(F.col("value").cast("string"), "$.payload.before.id").cast("int").alias("before_id"),
    )
    bronze_df.writeTo("lakehouse.cdc.bronze_customers").createOrReplace()
    return spark.table("lakehouse.cdc.bronze_customers").count()


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
