from datetime import datetime, timedelta
import os, json, subprocess, sys

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor

BOOTSTRAP  = 'kafka:9092'
TOPIC      = 'dbserver1.public.customers'
CONNECTOR  = 'pg-cdc-connector'
STATE_FILE = '/opt/airflow/dags/cdc_state.json'


def _pip(pkg, import_as=None):
    import importlib.util
    if importlib.util.find_spec(import_as or pkg) is None:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-q', pkg])


# ── BranchPythonOperator ─────────────────────────────────────────────────────

def check_new_events(**ctx):
    """Compare latest Kafka offset to stored offset; return next task_id."""
    _pip('kafka-python-ng', 'kafka')
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    parts = consumer.partitions_for_topic(TOPIC) or set()
    if not parts:
        consumer.close()
        ctx['ti'].xcom_push(key='new_count', value=0)
        return 'skip_merge'

    tp = TopicPartition(TOPIC, 0)
    consumer.assign([tp])
    consumer.seek_to_end(tp)
    latest = consumer.position(tp)
    consumer.close()

    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)

    stored    = state.get('last_offset', 0)
    new_count = max(0, latest - stored)
    print('Kafka offsets: latest=%d  stored=%d  new=%d' % (latest, stored, new_count))

    ctx['ti'].xcom_push(key='from_offset', value=stored)
    ctx['ti'].xcom_push(key='to_offset',   value=latest)
    ctx['ti'].xcom_push(key='new_count',   value=new_count)
    return 'bronze_cdc' if new_count > 0 else 'skip_merge'


# ── Bronze CDC ───────────────────────────────────────────────────────────────

def run_bronze(**ctx):
    """Read new Kafka events; write Bronze JSON checkpoint (idempotent by logical date)."""
    _pip('kafka-python-ng', 'kafka')
    from kafka import KafkaConsumer, TopicPartition

    ti          = ctx['ti']
    from_offset = ti.xcom_pull(key='from_offset', task_ids='check_new_events')
    to_offset   = ti.xcom_pull(key='to_offset',   task_ids='check_new_events')

    if from_offset is None or from_offset >= to_offset:
        print('Nothing to read')
        return 0

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
    )
    tp = TopicPartition(TOPIC, 0)
    consumer.assign([tp])
    consumer.seek(tp, from_offset)

    events = []
    while consumer.position(tp) < to_offset:
        batch = consumer.poll(timeout_ms=3000, max_records=500)
        if not batch:
            break
        for msgs in batch.values():
            for msg in msgs:
                if msg.offset < to_offset and msg.value:
                    events.append({'offset': msg.offset, 'value': msg.value.decode()})
    consumer.close()

    # Key the file by logical date -> re-running the same logical date overwrites safely
    ds          = ctx['ds_nodash']
    bronze_path = '/opt/airflow/dags/bronze_' + ds + '.json'
    with open(bronze_path, 'w') as f:
        json.dump({'from': from_offset, 'to': to_offset, 'events': events}, f)

    with open(STATE_FILE, 'w') as f:
        json.dump({'last_offset': to_offset, 'last_run': ctx['ds']}, f)

    print('Bronze: %d events -> %s' % (len(events), bronze_path))
    return len(events)


# ── Silver CDC ───────────────────────────────────────────────────────────────

def run_silver(**ctx):
    """Apply Bronze events to silver_customers_af via PostgreSQL ON CONFLICT upsert."""
    _pip('psycopg2-binary', 'psycopg2')
    import psycopg2

    ds          = ctx['ds_nodash']
    bronze_path = '/opt/airflow/dags/bronze_' + ds + '.json'
    if not os.path.exists(bronze_path):
        print('No bronze file for ' + ds)
        return 0

    with open(bronze_path) as f:
        data = json.load(f)

    conn = psycopg2.connect(
        host='postgres', port=5432, dbname='sourcedb',
        user=os.environ.get('PG_USER', 'cdc_user'),
        password=os.environ.get('PG_PASSWORD', 'admin'),
    )
    conn.autocommit = True
    cur = conn.cursor()

    create_ddl = (
        'CREATE TABLE IF NOT EXISTS silver_customers_af ('
        'id INTEGER PRIMARY KEY, '
        'name VARCHAR(100), '
        'email VARCHAR(200), '
        'country VARCHAR(50), '
        'updated_at TIMESTAMP DEFAULT NOW())'
    )
    cur.execute(create_ddl)

    upsert_sql = (
        'INSERT INTO silver_customers_af (id, name, email, country, updated_at) '
        'VALUES (%s, %s, %s, %s, NOW()) '
        'ON CONFLICT (id) DO UPDATE SET '
        'name=EXCLUDED.name, email=EXCLUDED.email, '
        'country=EXCLUDED.country, updated_at=EXCLUDED.updated_at'
    )

    applied = 0
    for evt in data.get('events', []):
        try:
            payload = json.loads(evt['value']).get('payload', {})
            op     = payload.get('op')
            after  = payload.get('after')  or {}
            before = payload.get('before') or {}

            if op in ('c', 'r', 'u') and after.get('id'):
                cur.execute(upsert_sql,
                            (after['id'], after.get('name'),
                             after.get('email'), after.get('country')))
                applied += 1
            elif op == 'd' and before.get('id'):
                cur.execute('DELETE FROM silver_customers_af WHERE id=%s', (before['id'],))
                applied += 1
        except Exception as e:
            print('  skip offset=%s: %s' % (evt.get('offset'), e))

    cur.close()
    conn.close()
    print('Silver: %d operations applied' % applied)
    return applied


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'sla': timedelta(minutes=30),
}

with DAG(
    dag_id='cdc_pipeline',
    description='CDC: PostgreSQL -> Debezium -> Kafka -> Bronze -> Silver',
    schedule_interval='@hourly',
    start_date=datetime(2026, 4, 19, 0, 0, 0),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['cdc', 'debezium', 'week06'],
) as dag:

    health_check = HttpSensor(
        task_id='health_check',
        http_conn_id='debezium_connect',
        endpoint='/connectors/' + CONNECTOR + '/status',
        response_check=lambda r: r.json().get('connector', {}).get('state') == 'RUNNING',
        poke_interval=20,
        timeout=300,
        mode='poke',
    )

    branch = BranchPythonOperator(
        task_id='check_new_events',
        python_callable=check_new_events,
    )

    bronze = PythonOperator(
        task_id='bronze_cdc',
        python_callable=run_bronze,
    )

    silver = PythonOperator(
        task_id='silver_cdc',
        python_callable=run_silver,
    )

    skip = EmptyOperator(task_id='skip_merge')

    health_check >> branch >> [bronze, skip]
    bronze >> silver
