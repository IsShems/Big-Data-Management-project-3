"""
Build Project 3 custom gold tipping tables from taxi/cdc silver layers.

Outputs:
  - lakehouse.taxi.gold_tipping_behavior
  - lakehouse.taxi.gold_tip_rankings
  - lakehouse.taxi.gold_driver_tip_report
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_spark import new_session


def run(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.taxi")

    spark.sql(
        """
        CREATE OR REPLACE TABLE lakehouse.taxi.gold_tipping_behavior USING iceberg AS
        WITH base AS (
          SELECT
            t.trip_id,
            t.PULocationID,
            hour(t.tpep_pickup_datetime) AS pickup_hour,
            t.fare_amount,
            t.tip_amount,
            t.payment_type,
            CASE
              WHEN t.fare_amount > 0 THEN (t.tip_amount / t.fare_amount) * 100
              ELSE NULL
            END AS tip_pct
          FROM lakehouse.taxi.silver_trips t
          WHERE t.fare_amount > 0
        ),
        high_tip_mode AS (
          SELECT
            PULocationID,
            pickup_hour,
            payment_type,
            ROW_NUMBER() OVER (
              PARTITION BY PULocationID, pickup_hour
              ORDER BY COUNT(*) DESC, payment_type
            ) AS rn
          FROM base
          WHERE tip_pct > 20
          GROUP BY PULocationID, pickup_hour, payment_type
        )
        SELECT
          b.PULocationID,
          b.pickup_hour,
          AVG(b.tip_amount) AS avg_tip_amount,
          AVG(b.tip_pct) AS avg_tip_percentage,
          100.0 * AVG(CASE WHEN b.tip_amount = 0 THEN 1 ELSE 0 END) AS pct_zero_tip,
          100.0 * AVG(CASE WHEN b.tip_pct > 20 THEN 1 ELSE 0 END) AS pct_high_tip,
          m.payment_type AS high_tip_common_payment_type,
          COUNT(*) AS trips_count
        FROM base b
        LEFT JOIN high_tip_mode m
          ON b.PULocationID = m.PULocationID
         AND b.pickup_hour = m.pickup_hour
         AND m.rn = 1
        GROUP BY b.PULocationID, b.pickup_hour, m.payment_type
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE lakehouse.taxi.gold_tip_rankings USING iceberg AS
        WITH zone_day AS (
          SELECT
            date(tpep_pickup_datetime) AS trip_date,
            PULocationID,
            AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100 END) AS avg_tip_pct
          FROM lakehouse.taxi.silver_trips
          WHERE fare_amount > 0
          GROUP BY date(tpep_pickup_datetime), PULocationID
        ),
        ranked AS (
          SELECT
            trip_date,
            PULocationID,
            avg_tip_pct,
            ROW_NUMBER() OVER (PARTITION BY trip_date ORDER BY avg_tip_pct DESC) AS rn_top,
            ROW_NUMBER() OVER (PARTITION BY trip_date ORDER BY avg_tip_pct ASC) AS rn_bottom
          FROM zone_day
        ),
        best_hour AS (
          SELECT
            date(tpep_pickup_datetime) AS trip_date,
            hour(tpep_pickup_datetime) AS best_hour_global,
            AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100 END) AS hour_avg_tip_pct,
            ROW_NUMBER() OVER (
              PARTITION BY date(tpep_pickup_datetime)
              ORDER BY AVG(CASE WHEN fare_amount > 0 THEN (tip_amount / fare_amount) * 100 END) DESC
            ) AS rn
          FROM lakehouse.taxi.silver_trips
          WHERE fare_amount > 0
          GROUP BY date(tpep_pickup_datetime), hour(tpep_pickup_datetime)
        )
        SELECT
          r.trip_date,
          CASE WHEN r.rn_top <= 10 THEN 'top10' ELSE 'bottom10' END AS ranking_bucket,
          r.PULocationID,
          r.avg_tip_pct,
          h.best_hour_global,
          h.hour_avg_tip_pct AS best_hour_avg_tip_pct
        FROM ranked r
        JOIN best_hour h
          ON r.trip_date = h.trip_date
         AND h.rn = 1
        WHERE r.rn_top <= 10 OR r.rn_bottom <= 10
        """
    )

    spark.sql(
        """
        CREATE OR REPLACE TABLE lakehouse.taxi.gold_driver_tip_report USING iceberg AS
        WITH trip_driver AS (
          -- Synthetic mapping because taxi source does not include driver_id.
          SELECT
            t.trip_id,
            t.fare_amount,
            t.tip_amount,
            d.id AS driver_id,
            d.rating AS driver_rating
          FROM lakehouse.taxi.silver_trips t
          CROSS JOIN (SELECT COUNT(*) AS cnt FROM lakehouse.cdc.silver_drivers) dc
          JOIN lakehouse.cdc.silver_drivers d
            ON pmod(t.trip_id, dc.cnt) + 1 = d.id
          WHERE t.fare_amount > 0
        )
        SELECT
          driver_id,
          driver_rating,
          AVG(tip_amount) AS avg_tip_amount,
          AVG((tip_amount / fare_amount) * 100) AS avg_tip_pct,
          COUNT(*) AS trips_count
        FROM trip_driver
        GROUP BY driver_id, driver_rating
        """
    )


def main() -> None:
    spark = new_session("project3_taxi_gold")
    spark.sparkContext.setLogLevel("WARN")
    try:
        run(spark)
        print("Gold tables refreshed:")
        print("  lakehouse.taxi.gold_tipping_behavior")
        print("  lakehouse.taxi.gold_tip_rankings")
        print("  lakehouse.taxi.gold_driver_tip_report")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
