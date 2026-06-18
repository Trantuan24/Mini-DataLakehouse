"""Bronze tests: row counts must match the source CSV files."""
import os
import csv
import pytest

from common.config import (CSV_TO_TABLE, DATASET_DIR, INCREMENTAL_TABLES,
                           PG_PROPERTIES, PG_SOURCE_SCHEMA, pg_jdbc_url)


def _csv_row_count(path):
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # minus header


def _source_row_count(spark, table):
    query = f"(SELECT COUNT(*) AS c FROM {PG_SOURCE_SCHEMA}.{table}) AS src"
    try:
        row = (spark.read.format("jdbc")
               .option("url", pg_jdbc_url())
               .option("dbtable", query)
               .options(**PG_PROPERTIES)
               .load()
               .collect()[0])
        return int(row["c"])
    except Exception:
        return None


# fact-like tables that simulate_source.py replays month by month; in replay
# (Phase 1) demos bronze holds only the months loaded so far, NOT the full CSV.
REPLAY_TABLES = {"orders", "order_items", "order_payments", "order_reviews"}


@pytest.mark.parametrize("csv_name,table", list(CSV_TO_TABLE.items()))
def test_bronze_rowcount_matches_csv(spark, csv_name, table):
    path = os.path.join(DATASET_DIR, csv_name)
    if not os.path.exists(path):
        pytest.skip(f"missing raw file {csv_name}")

    csv_count = _csv_row_count(path)
    source_count = _source_row_count(spark, table)
    actual = spark.table(f"bronze.{table}").count()

    if source_count is None:
        if os.environ.get("REPLAY_MODE") == "1" and table in REPLAY_TABLES:
            pytest.skip(f"replay mode: {table} holds only replayed months, not full CSV")
        assert actual == csv_count, f"{table}: bronze={actual} csv={csv_count}"
        return

    if table in INCREMENTAL_TABLES and source_count < csv_count:
        assert actual >= source_count, f"{table}: bronze={actual} source={source_count}"
    else:
        assert actual == source_count, f"{table}: bronze={actual} source={source_count}"
