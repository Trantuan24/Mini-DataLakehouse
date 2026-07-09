"""Lightweight data-quality framework (Great Expectations-style).

Each check returns a dict matching the dq_results schema from the spec:
    table_name, layer, check_name, passed, value, threshold, run_timestamp
Results are persisted to the Iceberg table `meta.dq_results` and a summary is
printed. If any check in a suite fails, run_suite() raises -> the Airflow task
fails and the pipeline stops (DQ gate)."""
from datetime import datetime
from pyspark.sql import SparkSession, Row

from .iceberg import append_iceberg, create_or_replace_iceberg


class DQError(Exception):
    pass


def _result(table, layer, check, passed, value=0.0, threshold=0.0):
    return {
        "table_name": table,
        "layer": layer,
        "check_name": check,
        "passed": bool(passed),
        "value": float(value),
        "threshold": float(threshold),
        "run_timestamp": datetime.utcnow(),
    }


# ---- individual expectations -------------------------------------------------

def expect_row_count_gt(df, n=0, *, table, layer):
    cnt = df.count()
    return _result(table, layer, f"row_count_gt_{n}", cnt > n, cnt, n)


def expect_column_not_null(df, column, *, table, layer):
    nulls = df.filter(df[column].isNull()).count()
    return _result(table, layer, f"{column}_not_null", nulls == 0, nulls, 0)


def expect_unique(df, columns, *, table, layer):
    total = df.count()
    distinct = df.select(*columns).distinct().count()
    dup = total - distinct
    return _result(table, layer, f"unique_{'_'.join(columns)}", dup == 0, dup, 0)


def expect_column_between(df, column, low, high, *, table, layer):
    bad = df.filter((df[column] < low) | (df[column] > high)).count()
    return _result(table, layer, f"{column}_between_{low}_{high}", bad == 0, bad, 0)


def expect_column_values_in_set(df, column, allowed, *, table, layer):
    bad = df.filter(df[column].isNotNull() & ~df[column].isin(*list(allowed))).count()
    return _result(table, layer, f"{column}_accepted_values", bad == 0, bad, 0)


def expect_column_positive(df, column, *, table, layer):
    bad = df.filter(df[column] <= 0).count()
    return _result(table, layer, f"{column}_positive", bad == 0, bad, 0)


def expect_no_orphans(child_df, child_key, parent_df, parent_key, *, table, layer):
    """Every child_key must exist in parent_key (FK integrity)."""
    orphans = child_df.join(
        parent_df.select(parent_key).distinct(),
        child_df[child_key] == parent_df[parent_key],
        "left_anti",
    ).count()
    return _result(table, layer, f"fk_{child_key}_in_{parent_key}", orphans == 0, orphans, 0)


# ---- suite runner ------------------------------------------------------------

def run_suite(spark: SparkSession, layer: str, results: list, *, persist=True):
    """Print + persist results, raise DQError if any failed."""
    print(f"\n===== Data Quality report [{layer}] =====")
    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"  [{status}] {r['table_name']}.{r['check_name']} "
              f"(value={r['value']}, threshold={r['threshold']})")

    if persist:
        try:
            spark.sql("CREATE DATABASE IF NOT EXISTS meta")
            rows = spark.createDataFrame([Row(**r) for r in results])
            if spark.catalog.tableExists("meta.dq_results"):
                append_iceberg(rows, "meta.dq_results")
            else:
                create_or_replace_iceberg(rows, "meta.dq_results")
        except Exception as e:  # don't let logging break the gate
            print(f"  (warning) could not persist dq_results: {e}")

    failed = [r for r in results if not r["passed"]]
    if failed:
        names = ", ".join(f"{r['table_name']}.{r['check_name']}" for r in failed)
        raise DQError(f"{len(failed)} data-quality check(s) FAILED at {layer}: {names}")
    print(f"===== All {len(results)} checks passed at [{layer}] =====\n")
