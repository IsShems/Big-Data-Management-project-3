
# Big Data Management: Project 1 - NYC Taxi Incremental ETL

## 1. Correctness

### Data Pipeline Flow
This pipeline implements an incremental ingestion pattern using a `manifest.json` state file to track processed inputs.

**Row Counts (Initial Run - January 2023):**
* **Raw Input:** 3,066,766
* **After Cleaning:** 2,995,023
* **After Deduplication:** 2,995,001
* **Final Output (Outbox):** 2,995,001

### "Bad Row" Examples & Handling
I implemented three primary cleaning rules to ensure high data quality:
1.  **Metric Validation:** Rows with `trip_distance <= 0` or `passenger_count <= 0` were dropped. These represent GPS errors or system "pings" that do not constitute real economic activity.
2.  **Timestamp Integrity:** Any row with a `NULL` value in pickup or dropoff timestamps was removed, as these prevent the calculation of `trip_duration_minutes`.
3.  **Deduplication:** I defined a unique key as `(VendorID, tpep_pickup_datetime)`. If multiple rows shared this key, only the first occurrence was kept to prevent double-counting from source retries.



---

## 2. Performance

### Runtime & Execution
* **Total Runtime:** ~45 seconds (including Spark environment initialization).
* **ETL Core Logic:** ~12-15 seconds for processing 3.06M records.

### Optimization Choices
1.  **Broadcast Hash Join:** The `taxi_zone_lookup` table is a small dimension table (~260 rows). By using `F.broadcast()`, I replicated the table to all worker nodes, avoiding a costly Shuffle Hash Join and reducing network overhead by nearly 100% for the join stage.
2.  **Column Pruning & Manifest Logic:** I explicitly selected only the required columns (`LocationID`, `Zone`) before the join. Combined with the `manifest.json` check, the system achieves "Zero-I/O" for previously processed files during re-runs.



---

## 3. Scenario: Weekday vs. Weekend Analysis

### Solution
The custom scenario required analyzing trip patterns based on the day of the week. 
* **Logic:** I utilized the `dayofweek` function on the pickup timestamp. Sunday (1) and Saturday (7) were categorized as **"Weekend"**, while values 2-6 were categorized as **"Weekday"**.
* **Correctness Fix:** I applied an explicit cast to `TimestampType` for the pickup column to ensure the `dayofweek` function resolved correctly, eliminating initial `NULL` issues.

**Final Summary Table:**
| Day Type | Total Trips | Average Fare |
| :--- | :--- | :--- |
| **Weekday** | 2,424,602 | 18.21 |
| **Weekend** | 570,399 | 18.25 |