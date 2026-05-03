"""
Batch: lakehouse.cdc.bronze_customers -> lakehouse.cdc.silver_customers (current state).
"""
from __future__ import annotations

import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_spark import new_session


def run(spark) -> int:
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


def main() -> None:
    spark = new_session("session6_silver_cdc")
    spark.sparkContext.setLogLevel("WARN")
    try:
        n = run(spark)
        print(f"Silver customers rows: {n}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
