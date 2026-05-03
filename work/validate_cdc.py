"""
Validation checks for Project 3 CDC mirrors.

Compares row counts between PostgreSQL source and Iceberg silver tables.
Fails with non-zero exit code on mismatch.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from lakehouse_spark import new_session


def _ensure(pkg: str, import_name: str | None = None) -> None:
    import importlib.util
    import subprocess

    if importlib.util.find_spec(import_name or pkg) is None:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])


def _pg_count(table: str) -> int:
    _ensure("psycopg2-binary", "psycopg2")
    import psycopg2

    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "postgres"),
        port=int(os.environ.get("PG_PORT", 5432)),
        dbname=os.environ.get("PG_DB", "sourcedb"),
        user=os.environ.get("PG_USER", "cdc_user"),
        password=os.environ.get("PG_PASSWORD", "cdc_pass"),
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    c = int(cur.fetchone()[0])
    cur.close()
    conn.close()
    return c


def main() -> None:
    spark = new_session("project3_validate_cdc")
    spark.sparkContext.setLogLevel("WARN")
    try:
        pg_customers = _pg_count("customers")
        pg_drivers = _pg_count("drivers")

        sf_customers = spark.table("lakehouse.cdc.silver_customers").count()
        sf_drivers = spark.table("lakehouse.cdc.silver_drivers").count()

        print("Validation counts:")
        print(f"  postgres.customers={pg_customers} | silver_customers={sf_customers}")
        print(f"  postgres.drivers  ={pg_drivers} | silver_drivers  ={sf_drivers}")

        assert sf_customers == pg_customers, "customers count mismatch"
        assert sf_drivers == pg_drivers, "drivers count mismatch"
        print("CDC validation OK")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
