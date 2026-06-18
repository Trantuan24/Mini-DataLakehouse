"""Mini Lakehouse end-to-end batch pipeline.

Postgres(olist_source) -> Bronze -> Silver -> Gold -> Platinum, with a
self-written (GE-style) DQ gate after each layer and a pytest stage at the end.

Seeding the Postgres source is a separate one-off DAG (`seed_source_postgres`);
this analytics pipeline assumes the source already exists and starts at the
Bronze ingest.

Each Spark job is submitted to the standalone cluster (SPARK_MASTER_URL set in
the environment); the Spark driver runs inside the airflow-scheduler container."""
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

PIPELINE = "/opt/pipeline"


def alert_on_failure(context):
    """Lightweight failure alert: log the failed task + run_id so a red DAG is
    easy to triage. Dependency-free on purpose (no SMTP/Slack) -- the real
    pipeline gate is the DQ/pytest task failing the run; this just annotates it.
    Swap the log line for an email/Slack call if a channel is configured."""
    ti = context.get("task_instance")
    dag = context.get("dag")
    logging.error(
        "[ALERT] task FAILED: dag=%s task=%s run_id=%s try=%s log=%s",
        dag.dag_id if dag else "?",
        ti.task_id if ti else "?",
        context.get("run_id"),
        ti.try_number if ti else "?",
        ti.log_url if ti else "?",
    )


default_args = {
    "owner": "tranduytuan",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,
}


def spark_task(task_id, module_path):
    # RUN_ID = the Airflow dag run_id, shared by every Spark job of one run so
    # their meta.job_log rows can be correlated. append_env keeps PATH/Spark env.
    return BashOperator(
        task_id=task_id,
        bash_command=f"spark-submit {PIPELINE}/{module_path}",
        env={"RUN_ID": "{{ run_id }}"},
        append_env=True,
    )


with DAG(
    dag_id="lakehouse_pipeline",
    description="Olist Mini Lakehouse: Bronze->Silver->Gold->Platinum",
    default_args=default_args,
    schedule_interval=None,  # manual trigger only -> deterministic; never fires
                             # before the source DB has been seeded
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "olist", "iceberg"],
) as dag:

    # source seeding lives in the one-off `seed_source_postgres` DAG; the
    # analytics pipeline starts straight at the Bronze ingest. Tasks are grouped
    # by medallion layer (TaskGroup) so the graph view maps to the architecture.
    with TaskGroup(group_id="bronze") as bronze:
        ingest = spark_task("ingest_raw_to_bronze", "bronze/ingest.py")
        validate = spark_task("validate_bronze", "bronze/validate.py")
        ingest >> validate

    with TaskGroup(group_id="silver") as silver:
        transform = spark_task("transform_bronze_to_silver", "silver/transform.py")
        validate = spark_task("validate_silver", "silver/validate.py")
        transform >> validate

    with TaskGroup(group_id="gold") as gold:
        dims = spark_task("build_gold_dims", "gold/build_dimensions.py")
        facts = spark_task("build_gold_facts", "gold/build_facts.py")
        validate = spark_task("validate_gold", "gold/validate.py")
        dims >> facts >> validate

    with TaskGroup(group_id="platinum") as platinum:
        spark_task("build_platinum", "platinum/build_marts.py")

    # [extension #2] pytest ETL checks. In a replay demo (trigger conf
    # {"replay": "1"}) the bronze rowcount test for the replayed fact tables is
    # skipped, since bronze then holds only the months loaded so far.
    run_tests = BashOperator(
        task_id="run_etl_tests",
        bash_command="pytest -q -o cache_dir=/tmp/pytest_cache /opt/tests",
        env={"REPLAY_MODE": "{{ dag_run.conf.get('replay', '0') }}"},
        append_env=True,
    )

    notify_done = EmptyOperator(task_id="notify_done")

    bronze >> silver >> gold >> platinum >> run_tests >> notify_done
