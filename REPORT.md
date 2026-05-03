# REPORT — Project 3

## 1) CDC Correctness

### Architecture

I implemented CDC from PostgreSQL (`customers`, `drivers`) using Debezium + Kafka and loaded it into Iceberg:

- Bronze CDC: `lakehouse.cdc.bronze_customers`, `lakehouse.cdc.bronze_drivers`
- Silver CDC: `lakehouse.cdc.silver_customers`, `lakehouse.cdc.silver_drivers`

Bronze stores raw Debezium-derived fields (`op`, `before`, `after`, offsets, timestamps).  
Silver stores the latest state per primary key (current-state mirror).

### Correctness checks performed

- Compared PostgreSQL and Iceberg counts in notebook (`project3_evidence.ipynb`).
- Spot-checked sample rows from `silver_customers` and `silver_drivers`.
- Confirmed CDC path handles both tables after connector fix.

Final evidence state:

- `postgres.drivers = 8`
- `silver_drivers = 4` (after generated CDC events + rebuild)
- `gold_driver_tip_report = 3`

This confirms end-to-end driver CDC now lands in silver and is consumed by gold.

### Idempotency

The DAG uses Kafka offset high-water state (`dags/cdc_state.json`) and a branch task:

- if no new Kafka events: route to `skip_merge`
- if new events exist: run `bronze_cdc -> silver_cdc`

Re-running without new events does not duplicate state changes in silver.

## 2) Lakehouse Design

### Medallion tables

- CDC Bronze: raw events with metadata and payload extracts
- CDC Silver: deduplicated latest-state mirrors
- Taxi Bronze: raw taxi trips from parquet
- Taxi Silver: cleaned/enriched trips (+ zone enrichment, derived metrics)
- Taxi Gold:
  - `lakehouse.taxi.gold_tipping_behavior`
  - `lakehouse.taxi.gold_tip_rankings`
  - `lakehouse.taxi.gold_driver_tip_report`

### Iceberg snapshots and time travel

In `project3_evidence.ipynb`, I queried:

- `lakehouse.cdc.silver_customers.snapshots`
- time travel via `VERSION AS OF <previous_snapshot_id>`

This demonstrates rollback capability for bad merges.

## 3) Orchestration Design (Airflow)

### DAG

- DAG ID: `project3_pipeline`
- Schedule: `*/15 * * * *` (15-minute freshness target)
- Retries: `3`, exponential backoff enabled
- SLA: 30 minutes
- Max active runs: 1

### Dependency chain

- `health_check -> check_new_events -> [bronze_cdc, skip_merge]`
- `bronze_cdc -> silver_cdc -> record_cdc_kafka_offset`
- `bronze_taxi -> silver_taxi -> gold_taxi`
- `[record_cdc_kafka_offset, gold_taxi] -> validate`

Rationale:

- Health check fails fast if connector is down.
- Branch prevents unnecessary CDC compute when no new events.
- Taxi path runs independently and converges in `validate`.

### Failure handling and observed behavior

- When no new CDC events exist, `bronze_cdc/silver_cdc/validate` can appear as skipped (expected with branch logic + default trigger rules).
- Retries/backoff are configured at DAG default args.
- I captured DAG graph and run history screenshots, including successful runs.

## 4) Taxi Streaming Pipeline (Project 2 improvements)

Improvements applied:

- Correct parquet ingestion scope in `work/taxi_bronze.py` (trip files only).
- Enriched silver with zone lookup and additional quality filters in `work/taxi_silver.py`.
- Added derived metrics (`trip_duration_minutes`, `trip_speed_kmh`).
- Built custom gold scenario tables in `work/taxi_gold.py`.

Taxi bronze/silver/gold tables were built successfully and queried in notebook evidence.

## 5) Custom Scenario (GitHub issue)

Scenario: tipping behavior + driver incentive analysis.

Implemented:

1. `gold_tipping_behavior` by zone + hour with tip KPIs
2. `gold_tip_rankings` for top/bottom tip zones and best hour
3. `gold_driver_tip_report` to study relation between driver rating and tips

For driver correlation, I used a documented synthetic trip-to-driver mapping because source taxi trips do not contain `driver_id`.

## 6) Operational Notes / Issues Resolved

Main issues resolved during implementation:

- Spark/Iceberg/Kafka package version mismatch for Spark 3.5 (fixed to `_2.12` artifacts).
- Airflow UI/API authentication alignment.
- Connector/table scope initially missing `drivers` CDC.
- Driver CDC recovery by generating new driver-side change events and rebuilding bronze/silver/gold.
- Notebook corruption (invalid BOM) fixed so `project3_evidence.ipynb` opens correctly.

## 7) Evidence Collected

Primary evidence source:

- `project3_evidence.ipynb` with screenshot markers for:
  - CDC correctness
  - Taxi silver/gold outputs
  - Custom scenario outputs
  - Airflow DAG graph/run history
  - Iceberg snapshot history + time travel proof
  - Final driver-related proof (`silver_drivers > 0`, `gold_driver_tip_report > 0`)

## 8) Environment Values Used (`.env`)

- `MINIO_ROOT_USER=admin`
- `MINIO_ROOT_PASSWORD=admin123`
- `PG_USER=cdc_user`
- `PG_PASSWORD=admin`
- `JUPYTER_TOKEN=admin`
- `AIRFLOW_USER=admin`
- `AIRFLOW_PASSWORD=admin`

## 9) Submission Checklist

- [x] End-to-end stack runs with Airflow, Jupyter, Kafka, Debezium, Iceberg
- [x] CDC bronze and silver tables implemented
- [x] Taxi bronze/silver/gold implemented and orchestrated
- [x] Airflow DAG graph + run history captured
- [x] Iceberg snapshots + time travel shown
- [x] Custom scenario queries implemented and evidenced
- [x] Final driver-related outputs non-empty after recovery flow