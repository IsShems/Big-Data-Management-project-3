-- Project 3 custom scenario prototype: tipping behavior + rankings + driver report.

-- 1) gold_tipping_behavior (zone x hour)
WITH tip_base AS (
    SELECT
        t.trip_id,
        t.PULocationID,
        hour(t.tpep_pickup_datetime) AS pickup_hour,
        t.fare_amount,
        t.tip_amount,
        t.payment_type,
        CASE WHEN t.fare_amount > 0 THEN (t.tip_amount / t.fare_amount) * 100 END AS tip_pct
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
    FROM tip_base
    WHERE tip_pct > 20
    GROUP BY PULocationID, pickup_hour, payment_type
)
SELECT
    b.PULocationID,
    b.pickup_hour,
    AVG(b.tip_amount) AS avg_tip_amount,
    AVG(b.tip_pct) AS avg_tip_percentage,
    100.0 * AVG(CASE WHEN coalesce(b.tip_amount, 0) = 0 THEN 1 ELSE 0 END) AS pct_zero_tip,
    100.0 * AVG(CASE WHEN b.tip_pct > 20 THEN 1 ELSE 0 END) AS pct_high_tip,
    m.payment_type AS high_tip_common_payment_type
FROM tip_base b
LEFT JOIN high_tip_mode m
  ON b.PULocationID = m.PULocationID
 AND b.pickup_hour = m.pickup_hour
 AND m.rn = 1
GROUP BY b.PULocationID, b.pickup_hour, m.payment_type
ORDER BY avg_tip_percentage DESC
LIMIT 20;

-- 2) best zone+hour combination for tips
WITH tip_base AS (
    SELECT
        t.PULocationID,
        hour(t.tpep_pickup_datetime) AS pickup_hour,
        CASE WHEN t.fare_amount > 0 THEN (t.tip_amount / t.fare_amount) * 100 END AS tip_pct
    FROM lakehouse.taxi.silver_trips t
    WHERE t.fare_amount > 0
)
SELECT
    PULocationID,
    pickup_hour,
    AVG(tip_pct) AS avg_tip_pct
FROM tip_base
GROUP BY PULocationID, pickup_hour
ORDER BY avg_tip_pct DESC
LIMIT 1;

-- 3) driver tip report (synthetic assignment via modulo, as taxi has no driver_id key)
WITH trip_driver AS (
    SELECT
        t.trip_id,
        t.fare_amount,
        t.tip_amount,
        d.id AS driver_id,
        d.rating AS driver_rating
    FROM lakehouse.taxi.silver_trips t
    CROSS JOIN (SELECT count(*) AS cnt FROM lakehouse.cdc.silver_drivers) dc
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
ORDER BY driver_rating DESC
LIMIT 20;
