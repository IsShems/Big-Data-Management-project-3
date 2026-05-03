"""
Session 6 — Single orchestrated DAG: Week 06 patterns (HttpSensor, branch, skip) + Project 3 Iceberg.

CDC path: HttpSensor -> BranchPythonOperator (Kafka offsets) -> Spark bronze/silver (Jupyter
docker exec) -> record high-water offset for the next branch decision.

Taxi path: bronze_taxi -> silver_taxi -> gold_taxi.

Week 06 lab notebook targets this DAG (dag_id=project3_pipeline); task ids match the course
(health_check, check_new_events, bronze_cdc, silver_cdc, skip_merge).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.http.sensors.http import HttpSensor

default_args = {
    "owner": "your-group",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "sla": timedelta(minutes=30),
}

CONNECT_URL = os.environ.get("CONNECT_URL", "http://connect:8083")
CONNECTOR_NAME = os.environ.get("CDC_CONNECTOR_NAME", "pg-cdc-connector")
JUPYTER = os.environ.get("JUPYTER_CONTAINER_NAME", "jupyter")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("CDC_KAFKA_TOPIC", "dbserver1.public.customers")
STATE_FILE = "/opt/airflow/dags/cdc_state.json"

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0"
)


def _pip(pkg: str, import_as: str | None = None) -> None:
    import importlib.util

    if importlib.util.find_spec(import_as or pkg) is None:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", pkg])


def check_new_events(**ctx):
    """Compare latest Kafka offset to stored offset; branch to bronze_cdc or skip_merge."""
    _pip("kafka-python-ng", "kafka")
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    parts = consumer.partitions_for_topic(TOPIC) or set()
    if not parts:
        consumer.close()
        ctx["ti"].xcom_push(key="new_count", value=0)
        return "skip_merge"

    tp = TopicPartition(TOPIC, 0)
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    latest = consumer.position(tp)
    consumer.close()

    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)

    stored = state.get("last_offset", 0)
    new_count = max(0, latest - stored)
    print("Kafka offsets: latest=%d  stored=%d  new=%d" % (latest, stored, new_count))

    ctx["ti"].xcom_push(key="from_offset", value=stored)
    ctx["ti"].xcom_push(key="to_offset", value=latest)
    ctx["ti"].xcom_push(key="new_count", value=new_count)
    return "bronze_cdc" if new_count > 0 else "skip_merge"


def record_kafka_highwater(**ctx):
    """After Spark CDC, persist topic high-water so the branch can skip empty runs."""
    _pip("kafka-python-ng", "kafka")
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    parts = consumer.partitions_for_topic(TOPIC) or set()
    if not parts:
        consumer.close()
        print("No partitions for topic; leaving state unchanged")
        return

    tp = TopicPartition(TOPIC, 0)
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    latest = consumer.position(tp)
    consumer.close()

    with open(STATE_FILE, "w") as f:
        json.dump({"last_offset": latest, "last_run": ctx.get("ds", "")}, f)
    try:
        os.chmod(STATE_FILE, 0o666)
    except OSError:
        pass
    print("Recorded Kafka high-water offset:", latest)


def _spark_submit(script: str) -> str:
    return f"""
    docker exec {JUPYTER} spark-submit \\
      --packages {SPARK_PACKAGES} \\
      /home/jovyan/project/work/{script}
    """.strip()


with DAG(
    dag_id="project3_pipeline",
    default_args=default_args,
    description="Project 3: CDC (Iceberg) + taxi — Week 06 orchestration merged here",
    start_date=datetime(2026, 4, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["project3", "session6", "week06"],
) as dag:
    health_check = HttpSensor(
        task_id="health_check",
        http_conn_id="debezium_connect",
        endpoint=f"/connectors/{CONNECTOR_NAME}/status",
        response_check=lambda r: r.json().get("connector", {}).get("state") == "RUNNING",
        poke_interval=20,
        timeout=300,
        mode="poke",
    )

    check_new_events = BranchPythonOperator(
        task_id="check_new_events",
        python_callable=check_new_events,
    )

    bronze_cdc = BashOperator(
        task_id="bronze_cdc",
        bash_command=_spark_submit("bronze.py"),
    )

    silver_cdc = BashOperator(
        task_id="silver_cdc",
        bash_command=_spark_submit("silver.py"),
    )

    record_cdc_offset = PythonOperator(
        task_id="record_cdc_kafka_offset",
        python_callable=record_kafka_highwater,
    )

    skip_merge = EmptyOperator(task_id="skip_merge")

    bronze_taxi = BashOperator(
        task_id="bronze_taxi",
        bash_command=_spark_submit("taxi_bronze.py"),
    )

    silver_taxi = BashOperator(
        task_id="silver_taxi",
        bash_command=_spark_submit("taxi_silver.py"),
    )

    gold_taxi = BashOperator(
        task_id="gold_taxi",
        bash_command=_spark_submit("taxi_gold.py"),
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=_spark_submit("validate_cdc.py"),
    )

    health_check >> check_new_events >> [bronze_cdc, skip_merge]
    bronze_cdc >> silver_cdc >> record_cdc_offset
    bronze_taxi >> silver_taxi >> gold_taxi
    [record_cdc_offset, gold_taxi] >> validate
