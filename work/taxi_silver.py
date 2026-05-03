"""
lakehouse.taxi.bronze_trips -> lakehouse.taxi.silver_trips (clean types, filter bad fares).
"""
from __future__ import annotations

import os
import sys

import pyspark.sql.functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_spark import new_session

TAXI_ZONE_LOOKUP = os.environ.get(
    "TAXI_ZONE_LOOKUP", "/home/jovyan/project/data/taxi_zone_lookup.parquet"
)


def _col(df, logical: str):
    cmap = {c.lower(): c for c in df.columns}
    key = logical.lower()
    if key not in cmap:
        raise ValueError(
            f"Column '{logical}' not found in bronze_trips; available: {df.columns}"
        )
    return F.col(cmap[key])


def run(spark) -> int:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")
    bronze = spark.table("lakehouse.taxi.bronze_trips")
    if not os.path.exists(TAXI_ZONE_LOOKUP):
        raise FileNotFoundError(f"Missing zone lookup parquet: {TAXI_ZONE_LOOKUP}")
    zones = spark.read.parquet(TAXI_ZONE_LOOKUP)

    trip_id = _col(bronze, "trip_id")
    pu = _col(bronze, "PULocationID")
    do = _col(bronze, "DOLocationID")
    fare = _col(bronze, "fare_amount").cast("double")
    tip = _col(bronze, "tip_amount").cast("double")
    payment_type = _col(bronze, "payment_type").cast("int")
    dist = _col(bronze, "trip_distance").cast("double")
    pickup = _col(bronze, "tpep_pickup_datetime")
    dropoff = _col(bronze, "tpep_dropoff_datetime")

    pickup_ts = F.to_timestamp(pickup.cast("string"))
    dropoff_ts = F.to_timestamp(dropoff.cast("string"))
    trip_duration_minutes = (
        F.unix_timestamp(dropoff_ts) - F.unix_timestamp(pickup_ts)
    ) / F.lit(60.0)
    invalid_speed = (
        trip_duration_minutes.isNull()
        | dist.isNull()
        | (trip_duration_minutes <= F.lit(0.0))
        | (dist <= F.lit(0.0))
    )
    trip_speed_kmh = F.when(invalid_speed, F.lit(None).cast("double")).otherwise(
        dist / (trip_duration_minutes / F.lit(60.0))
    )

    pu_zone = zones.select(
        F.col("LocationID").alias("_pu_id"),
        F.col("Zone").alias("pickup_zone"),
    )
    do_zone = zones.select(
        F.col("LocationID").alias("_do_id"),
        F.col("Zone").alias("dropoff_zone"),
    )

    silver_df = bronze.select(
        trip_id.alias("trip_id"),
        pu.cast("int").alias("PULocationID"),
        do.cast("int").alias("DOLocationID"),
        fare.alias("fare_amount"),
        tip.alias("tip_amount"),
        payment_type.alias("payment_type"),
        dist.alias("trip_distance"),
        pickup_ts.alias("tpep_pickup_datetime"),
        dropoff_ts.alias("tpep_dropoff_datetime"),
        trip_duration_minutes.alias("trip_duration_minutes"),
        trip_speed_kmh.alias("trip_speed_kmh"),
    ).filter(
        (F.col("fare_amount").isNotNull())
        & (F.col("tip_amount").isNotNull())
        & (F.col("trip_distance").isNotNull())
        & (F.col("tpep_pickup_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime"))
        & (F.col("fare_amount") > 0)
        & (F.col("tip_amount") >= 0)
        & (F.col("trip_distance") > 0)
    )
    silver_df = (
        silver_df.join(pu_zone, silver_df["PULocationID"] == pu_zone["_pu_id"], "left")
        .drop("_pu_id")
        .join(do_zone, silver_df["DOLocationID"] == do_zone["_do_id"], "left")
        .drop("_do_id")
    )

    silver_df.createOrReplaceTempView("taxi_silver_batch")
    spark.sql(
        """
        CREATE OR REPLACE TABLE lakehouse.taxi.silver_trips USING iceberg AS
        SELECT * FROM taxi_silver_batch
        """
    )
    return spark.table("lakehouse.taxi.silver_trips").count()


def main() -> None:
    spark = new_session("session6_taxi_silver")
    spark.sparkContext.setLogLevel("WARN")
    try:
        n = run(spark)
        print(f"Silver taxi trips: {n}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
