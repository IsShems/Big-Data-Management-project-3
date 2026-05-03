"""
Bronze: Debezium topic -> Iceberg (REST catalog).

This notebook image is Spark 3.5.0 + Scala 2.12. PYSPARK_SUBMIT_ARGS must use the
_2.12 Kafka and Iceberg runtime artifacts; _2.13 jars trigger
NoClassDefFoundError: scala/$less$colon$less when reading Kafka.

Session 6 batch twin: work/bronze.py (same catalog via work/lakehouse_spark.py).
"""
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "work"))
from lakehouse_spark import new_session

spark = new_session("CDC-Bronze")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.cdc")

raw = spark.read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "dbserver1.public.customers") \
  .option("startingOffsets", "earliest") \
  .load()

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

print("DONE: Bronze table created")
