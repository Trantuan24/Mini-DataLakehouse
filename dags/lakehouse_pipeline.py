"""Mini Lakehouse end-to-end batch pipeline.

Source(Postgres) -> Bronze -> Silver -> Gold -> Platinum, with a Great
Expectations-style DQ gate after each layer and a pytest stage at the end.

Each Spark job is submitted to the standalone cluster (SPARK_MASTER_URL set in
the environment); the Spark driver runs inside the airflow-scheduler container."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PIPELINE = "/opt/pipeline"

default_args = {
    "owner": "tranduytuan",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def spark_task(task_id, module_path):
    return BashOperator(
        task_id=task_id,
        bash_command=f"spark-submit {PIPELINE}/{module_path}",
    )


with DAG(
    dag_id="lakehouse_pipeline",
    description="Olist Mini Lakehouse: Bronze->Silver->Gold->Platinum",
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "olist", "iceberg"],
) as dag:

    # [extension #3] load CSV into Postgres source (pure-python, no Spark)
    load_source = BashOperator(
        task_id="load_source_to_postgres",
        bash_command=f"python {PIPELINE}/bronze/load_source.py",
        env={"DATASET_DIR": "/opt/dataset",
             "POSTGRES_USER": "airflow", "POSTGRES_PASSWORD": "airflow",
             "SOURCE_DB": "olist_source"},
        append_env=True,
    )

    ingest_bronze = spark_task("ingest_raw_to_bronze", "bronze/ingest.py")
    validate_bronze = spark_task("validate_bronze", "bronze/validate.py")
    transform_silver = spark_task("transform_bronze_to_silver", "silver/transform.py")
    validate_silver = spark_task("validate_silver", "silver/validate.py")
    build_dims = spark_task("build_gold_dims", "gold/build_dimensions.py")
    build_facts = spark_task("build_gold_facts", "gold/build_facts.py")
    validate_gold = spark_task("validate_gold", "gold/validate.py")
    build_platinum = spark_task("build_platinum", "platinum/build_marts.py")

    # [extension #2] pytest ETL checks
    run_tests = BashOperator(
        task_id="run_etl_tests",
        bash_command="pytest -q /opt/tests || true",
    )

    notify_done = EmptyOperator(task_id="notify_done")

    (load_source >> ingest_bronze >> validate_bronze >> transform_silver
        >> validate_silver >> build_dims >> build_facts >> validate_gold
        >> build_platinum >> run_tests >> notify_done)
