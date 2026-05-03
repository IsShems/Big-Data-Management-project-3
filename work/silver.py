"""
Batch: lakehouse.cdc bronze tables -> silver mirrors (customers + drivers).
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_spark import new_session


def run_customers(spark) -> int:
    spark.sql(
        """
        CREATE OR REPLACE TABLE lakehouse.cdc.silver_customers USING iceberg AS
        SELECT
          id,
          name,
          email,
          country,
          ts_ms AS updated_ts
        FROM (
          SELECT
            op,
            COALESCE(after_id, before_id) AS id,
            after_name AS name,
            after_email AS email,
            after_country AS country,
            ts_ms,
            kafka_offset,
            ROW_NUMBER() OVER (
              PARTITION BY COALESCE(after_id, before_id)
              ORDER BY ts_ms DESC, kafka_offset DESC
            ) AS rn
          FROM lakehouse.cdc.bronze_customers
          WHERE op IS NOT NULL
        ) s
        WHERE rn = 1
          AND op <> 'd'
          AND id IS NOT NULL
        """
    )
    return spark.table("lakehouse.cdc.silver_customers").count()


def run_drivers(spark) -> int:
    spark.sql(
        """
        CREATE OR REPLACE TABLE lakehouse.cdc.silver_drivers USING iceberg AS
        SELECT
          id,
          name,
          license_number,
          rating,
          city,
          active,
          ts_ms AS updated_ts
        FROM (
          SELECT
            op,
            COALESCE(after_id, before_id) AS id,
            after_name AS name,
            after_license_number AS license_number,
            after_rating AS rating,
            after_city AS city,
            after_active AS active,
            ts_ms,
            kafka_offset,
            ROW_NUMBER() OVER (
              PARTITION BY COALESCE(after_id, before_id)
              ORDER BY ts_ms DESC, kafka_offset DESC
            ) AS rn
          FROM lakehouse.cdc.bronze_drivers
          WHERE op IS NOT NULL
        ) s
        WHERE rn = 1
          AND op <> 'd'
          AND id IS NOT NULL
        """
    )
    return spark.table("lakehouse.cdc.silver_drivers").count()


def main() -> None:
    spark = new_session("session6_silver_cdc")
    spark.sparkContext.setLogLevel("WARN")
    try:
        c = run_customers(spark)
        d = run_drivers(spark)
        print(f"Silver customers rows: {c}")
        print(f"Silver drivers rows: {d}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
