from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
  .appName("CDC-Bronze") \
  .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.lakehouse.type", "rest") \
  .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181") \
  .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
  .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000") \
  .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true") \
  .config("spark.sql.defaultCatalog", "lakehouse") \
  .getOrCreate()

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
