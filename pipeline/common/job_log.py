"""Lightweight per-job run metrics -> Iceberg table `meta.job_log`.

Same spirit (and write pattern) as `meta.dq_results`: each Spark job wraps its
body in `with job_log(spark, layer, job_name) as log:` and sets `log.rows_out`.
One row is appended per job with:
    run_id, layer, job_name, status, rows_in, rows_out, duration_sec,
    error_msg, run_timestamp

`run_id` comes from the env var RUN_ID (the Airflow dag run_id, passed by the
DAG) so every Spark job of one pipeline run shares an id and can be correlated.
On failure the row is written with status='failed' + error_msg, then the
original exception is re-raised so the Airflow task still fails. Logging never
masks the real error (the write is best-effort)."""
import os
import time
from contextlib import contextmanager
from datetime import datetime

from pyspark.sql import Row

JOB_LOG_TABLE = "meta.job_log"


class _Log:
    """Mutable handle the job uses to report row counts."""
    def __init__(self):
        self.rows_in = 0
        self.rows_out = 0


def sum_counts(spark, tables):
    """Helper: total rows across a list of fully-qualified tables (best-effort;
    Iceberg count is metadata-fast). Missing tables count as 0."""
    total = 0
    for t in tables:
        try:
            total += spark.table(t).count()
        except Exception:
            pass
    return total


def _persist(spark, run_id, layer, job_name, status, rows_in, rows_out,
             duration_sec, error_msg):
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS meta")
        row = Row(run_id=run_id, layer=layer, job_name=job_name, status=status,
                  rows_in=int(rows_in), rows_out=int(rows_out),
                  duration_sec=float(duration_sec), error_msg=error_msg or "",
                  run_timestamp=datetime.utcnow())
        df = spark.createDataFrame([row])
        if spark.catalog.tableExists(JOB_LOG_TABLE):
            df.writeTo(JOB_LOG_TABLE).append()
        else:
            (df.writeTo(JOB_LOG_TABLE).using("iceberg")
               .tableProperty("format-version", "2").createOrReplace())
        print(f"[job_log] {layer}.{job_name} status={status} "
              f"rows_out={rows_out} {duration_sec}s run_id={run_id}")
    except Exception as e:  # never let logging break the job
        print(f"[job_log] (warning) could not write {JOB_LOG_TABLE}: {e}")


@contextmanager
def job_log(spark, layer, job_name):
    """Time the wrapped block, then append one row to meta.job_log."""
    run_id = os.environ.get("RUN_ID", "manual")
    log = _Log()
    t0 = time.time()
    status, err = "success", ""
    try:
        yield log
    except Exception as e:
        status, err = "failed", str(e)[:1000]
        raise
    finally:
        _persist(spark, run_id, layer, job_name, status,
                 log.rows_in, log.rows_out, round(time.time() - t0, 2), err)
