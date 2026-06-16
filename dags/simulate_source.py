"""[Phase 1] Manual DAG: advance the simulated OLTP source by ONE month.

Each trigger releases one purchase-month of real Olist orders (+ their items /
payments / reviews) into Postgres `olist_source`. Kept deliberately SEPARATE
from `lakehouse_pipeline`: the analytics pipeline must never mutate the source,
and re-running the pipeline must not advance the replay. Demo loop:

    (once)  trigger seed_source_postgres  with conf {"mode": "dims_only"}
    (tick)  trigger simulate_source       -> +1 month in the source
    (tick)  trigger lakehouse_pipeline     -> ingest -> ... -> platinum
    repeat the two ticks; bronze.orders grows -> Iceberg snapshots differ.

Optional: trigger with conf {"month": "2017-03"} to jump to a specific month."""
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
    dag_id="simulate_source",
    description="Replay one month of real Olist orders+children into Postgres olist_source",
    default_args=default_args,
    schedule_interval=None,  # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "olist", "simulate", "source"],
) as dag:

    advance_one_month = BashOperator(
        task_id="advance_one_month",
        bash_command=f"python {PIPELINE}/bronze/simulate_source.py",
        env={"DATASET_DIR": "/opt/dataset",
             "POSTGRES_USER": "airflow", "POSTGRES_PASSWORD": "airflow",
             "SOURCE_DB": "olist_source",
             # empty unless the trigger passes conf {"month": "YYYY-MM"}
             "SIM_MONTH": "{{ dag_run.conf.get('month', '') }}"},
        append_env=True,
    )
