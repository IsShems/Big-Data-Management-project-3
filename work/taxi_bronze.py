"""
Taxi parquet -> lakehouse.taxi.bronze_trips (Iceberg) + synthetic trip_id.
"""
from __future__ import annotations

import glob
import os
import sys

import pyspark.sql.functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_spark import new_session

TAXI_DATA_PATH = os.environ.get("TAXI_DATA_PATH", "/home/jovyan/project/data")


def run(spark) -> int:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")
    path = TAXI_DATA_PATH
    if not os.path.isdir(path):
        raise FileNotFoundError(
            f"Taxi data directory missing: {path}. Add parquet under ./data/."
        )
    # Only ingest trip parquet files into bronze_trips.
    # taxi_zone_lookup.parquet has a different schema and must not be mixed here.
    parquet_files = sorted(glob.glob(os.path.join(path, "yellow_tripdata_*.parquet")))
    if not parquet_files:
        raise FileNotFoundError(
            f"No trip parquet files found under {path}. "
            "Expected files like yellow_tripdata_2025-01.parquet."
        )
    raw = spark.read.parquet(*parquet_files)
    if not raw.take(1):
        raise RuntimeError(f"No rows read from parquet under {path}")
    staged = raw.withColumn("trip_id", F.monotonically_increasing_id())
    staged.createOrReplaceTempView("taxi_bronze_batch")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lakehouse.taxi.bronze_trips USING iceberg AS
        SELECT * FROM taxi_bronze_batch
        """
    )
    return spark.table("lakehouse.taxi.bronze_trips").count()


def main() -> None:
    spark = new_session("session6_taxi_bronze")
    spark.sparkContext.setLogLevel("WARN")
    try:
        n = run(spark)
        print(f"Bronze taxi trips: {n}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
