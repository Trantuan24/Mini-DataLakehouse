"""[Extension #3] One-off seed DAG.

Loads the raw Olist CSVs into the Postgres `olist_source` schema to simulate an
OLTP source system. Run this ONCE (manual trigger) to populate the source DB;
the analytics DAG `lakehouse_pipeline` then ingests from it incrementally and
never needs to re-seed."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PIPELINE = "/opt/pipeline"

default_args = {
    "owner": "tranduytuan",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="seed_source_postgres",
    description="One-off: load Olist CSVs into Postgres olist_source (OLTP seed)",
    default_args=default_args,
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "olist", "seed", "postgres"],
) as dag:

    load_source = BashOperator(
        task_id="load_source_to_postgres",
        bash_command=f"python {PIPELINE}/bronze/load_source.py",
        env={"DATASET_DIR": "/opt/dataset",
             "POSTGRES_USER": "airflow", "POSTGRES_PASSWORD": "airflow",
             "SOURCE_DB": "olist_source"},
        append_env=True,
    )
