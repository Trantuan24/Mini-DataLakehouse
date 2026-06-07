"""Bronze tests: row counts must match the source CSV files."""
import os
import csv
import pytest

from common.config import CSV_TO_TABLE, DATASET_DIR


def _csv_row_count(path):
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # minus header


@pytest.mark.parametrize("csv_name,table", list(CSV_TO_TABLE.items()))
def test_bronze_rowcount_matches_csv(spark, csv_name, table):
    path = os.path.join(DATASET_DIR, csv_name)
    if not os.path.exists(path):
        pytest.skip(f"missing raw file {csv_name}")
    expected = _csv_row_count(path)
    actual = spark.table(f"bronze.{table}").count()
    assert actual == expected, f"{table}: bronze={actual} csv={expected}"
