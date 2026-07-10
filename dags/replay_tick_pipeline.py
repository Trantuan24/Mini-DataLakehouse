"""Scheduled replay orchestrator.

Runs one replay tick safely:
  simulate_source -> lakehouse_pipeline

The branch guard prevents the source replay from advancing again when a prior
simulate_source run has not yet been followed by a successful lakehouse run.
It also stops cleanly after the replay cursor reaches the last order month in
the dataset.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pandas as pd
import pendulum
import psycopg2
from airflow import DAG
from airflow.models import DagRun
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule

DATASET_DIR = os.environ.get("DATASET_DIR", "/opt/dataset")
SOURCE_DB = os.environ.get("SOURCE_DB", "olist_source")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")
SCHEMA = "olist_source"
VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")


def _pg_conn():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname=SOURCE_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def _latest_dataset_month():
    orders_path = os.path.join(DATASET_DIR, "olist_orders_dataset.csv")
    orders = pd.read_csv(orders_path, usecols=["order_purchase_timestamp"])
    months = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce"
    ).dt.to_period("M")
    latest = months.max()
    return latest.to_timestamp().date()


def _current_sim_month():
    query = f"""
        SELECT to_regclass('{SCHEMA}.sim_state') IS NOT NULL AS exists
    """
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            exists = cur.fetchone()[0]
            if not exists:
                return None
            cur.execute(
                f"SELECT last_loaded_month FROM {SCHEMA}.sim_state WHERE id = 1"
            )
            row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def _source_orders_count():
    query = f"""
        SELECT to_regclass('{SCHEMA}.orders') IS NOT NULL AS exists
    """
    with _pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            exists = cur.fetchone()[0]
            if not exists:
                return 0
            cur.execute(f"SELECT count(*) FROM {SCHEMA}.orders")
            return cur.fetchone()[0]


@provide_session
def _latest_success_end(dag_id, session=None):
    run = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id, DagRun.state == DagRunState.SUCCESS)
        .order_by(DagRun.end_date.desc())
        .first()
    )
    return run.end_date if run else None


def choose_replay_action():
    """Pick the safe next step without mutating source or lakehouse data."""
    latest_sim_end = _latest_success_end("simulate_source")
    latest_pipeline_end = _latest_success_end("lakehouse_pipeline")
    if latest_sim_end and (
        latest_pipeline_end is None or latest_sim_end > latest_pipeline_end
    ):
        print(
            "simulate_source is ahead of lakehouse_pipeline; "
            "running pipeline only."
        )
        return "trigger_pipeline_only"

    current_month = _current_sim_month()
    latest_month = _latest_dataset_month()
    source_orders = _source_orders_count()
    print(f"replay cursor={current_month}, dataset latest={latest_month}")
    if current_month is None and source_orders > 0:
        print(
            "Source already has orders but replay cursor is empty; "
            "assuming a full seed/baseline and not advancing simulate_source."
        )
        return "no_more_months"
    if current_month is not None and current_month >= latest_month:
        print("Replay is already at the latest available source month.")
        return "no_more_months"

    return "trigger_simulate_source"


def log_no_more_months():
    current_month = _current_sim_month()
    latest_month = _latest_dataset_month()
    source_orders = _source_orders_count()
    print(
        "No replay tick executed: "
        f"cursor={current_month}, dataset_latest={latest_month}, "
        f"source_orders={source_orders}."
    )


default_args = {
    "owner": "tranduytuan",
    "retries": 0,
    "execution_timeout": timedelta(minutes=90),
}


with DAG(
    dag_id="replay_tick_pipeline",
    description="Scheduled safe replay tick: simulate_source then lakehouse_pipeline",
    default_args=default_args,
    schedule_interval="0 */2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=VN_TZ),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    tags=["lakehouse", "olist", "replay", "scheduled"],
) as dag:
    choose_action = BranchPythonOperator(
        task_id="choose_replay_action",
        python_callable=choose_replay_action,
    )

    trigger_simulate_source = TriggerDagRunOperator(
        task_id="trigger_simulate_source",
        trigger_dag_id="simulate_source",
        trigger_run_id="replay_tick__simulate__{{ run_id }}",
        conf={"lifecycle": "0"},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_pipeline_after_simulate = TriggerDagRunOperator(
        task_id="trigger_pipeline_after_simulate",
        trigger_dag_id="lakehouse_pipeline",
        trigger_run_id="replay_tick__pipeline__{{ run_id }}",
        conf={"replay": "1"},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_pipeline_only = TriggerDagRunOperator(
        task_id="trigger_pipeline_only",
        trigger_dag_id="lakehouse_pipeline",
        trigger_run_id="replay_tick__pipeline_catchup__{{ run_id }}",
        conf={"replay": "1"},
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    no_more_months = PythonOperator(
        task_id="no_more_months",
        python_callable=log_no_more_months,
    )

    done = EmptyOperator(
        task_id="done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    choose_action >> trigger_simulate_source >> trigger_pipeline_after_simulate >> done
    choose_action >> trigger_pipeline_only >> done
    choose_action >> no_more_months >> done
