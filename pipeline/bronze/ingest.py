"""Bronze ingest: load 9 sources into Iceberg tables under database `bronze`.

Source is configurable:
  INGEST_SOURCE=postgres (default, extension #3)  -> read from olist_source schema
  INGEST_SOURCE=csv                               -> read CSVs directly (fallback)

No business transforms. Two metadata columns are added to every table:
  _ingested_at (timestamp), _source_file (string).

Write modes:
  * Most tables: overwrite (idempotent full reload).
  * `orders` (INCREMENTAL_TABLES): append-only with a high-watermark on
    order_purchase_timestamp, partitioned by months(business date). Each run
    only pulls rows newer than the stored watermark, so re-runs are cheap and
    Iceberg keeps the full history (see demo_time_travel)."""
import os
import sys

sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit, col, to_timestamp
from common.spark_session import get_spark, ensure_databases
from common.job_log import job_log, sum_counts
from common.iceberg import append_iceberg, create_or_replace_iceberg
from common.config import (CSV_TO_TABLE, BRONZE_PARTITIONED, DATASET_DIR,
                           INCREMENTAL_TABLES, WATERMARK_COLUMN, PARTITION_COLUMN,
                           WATERMARK_TABLE,
                           pg_jdbc_url, PG_SOURCE_SCHEMA, PG_PROPERTIES)

INGEST_SOURCE = os.environ.get("INGEST_SOURCE", "postgres")
# sentinel watermark for a table that has never been ingested
DEFAULT_WATERMARK = "1970-01-01 00:00:00"


def read_source(spark, table, csv_name):
    if INGEST_SOURCE == "csv":
        path = f"{DATASET_DIR}/{csv_name}"
        print(f"  reading CSV {path}")
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    # postgres
    dbtable = f"{PG_SOURCE_SCHEMA}.{table}"
    print(f"  reading JDBC {dbtable}")
    return (spark.read.format("jdbc")
            .option("url", pg_jdbc_url())
            .option("dbtable", dbtable)
            .options(**PG_PROPERTIES)
            .load())


# ---- incremental (watermark) helpers ----------------------------------------

def _read_watermark(spark, table):
    """Return the stored high-watermark for `table`, or the epoch sentinel if it
    has never been ingested."""
    if not spark.catalog.tableExists(WATERMARK_TABLE):
        return DEFAULT_WATERMARK
    row = (spark.table(WATERMARK_TABLE)
                .filter(col("table_name") == table)
                .agg(F.max("watermark_value").alias("wm"))
                .collect())
    return row[0]["wm"] if row and row[0]["wm"] is not None else DEFAULT_WATERMARK


def _update_watermark(spark, table, new_value):
    """Upsert the high-watermark for `table` into meta.ingest_watermark."""
    rec = (spark.createDataFrame(
                [(table,)], "table_name string")
              .withColumn("watermark_value", F.lit(new_value).cast("timestamp"))
              .withColumn("updated_at", current_timestamp()))
    if spark.catalog.tableExists(WATERMARK_TABLE):
        current = spark.table(WATERMARK_TABLE).filter(col("table_name") != table)
        create_or_replace_iceberg(current.unionByName(rec), WATERMARK_TABLE)
    else:
        create_or_replace_iceberg(rec, WATERMARK_TABLE)


def ingest_incremental(spark, table, df):
    """Append only rows whose WATERMARK column is newer than the stored
    high-watermark, partitioning by the BUSINESS month (a separate column).

    The two columns differ once lifecycle replay is on: the watermark tracks
    when the source row last changed (source_updated_at) so delivered-UPDATEs
    are re-ingested as new versions, while the partition stays the purchase
    month so every version of an order lands in the same partition. In
    insert-only / full mode the two columns hold the same value, so behaviour
    is unchanged. Idempotent: a re-run with no newer rows appends nothing."""
    wm_col = WATERMARK_COLUMN[table]
    part_col = PARTITION_COLUMN.get(table, wm_col)
    # safety for sources that lack the watermark column (e.g. raw CSV mode)
    if wm_col not in df.columns:
        wm_col = part_col

    watermark = _read_watermark(spark, table)
    print(f"  watermark[{table}] on {wm_col} = {watermark}")
    wm_ts = to_timestamp(col(wm_col))
    new_rows = (df.filter(wm_ts > F.lit(watermark).cast("timestamp"))
                  .withColumn("_part_ts", to_timestamp(col(part_col))))
    cnt = new_rows.count()
    print(f"  {cnt:,} new row(s) after watermark")

    if not spark.catalog.tableExists(f"bronze.{table}"):
        create_or_replace_iceberg(
            new_rows,
            f"bronze.{table}",
            partitioned_by="months(_part_ts)",
        )
    elif cnt > 0:
        append_iceberg(new_rows, f"bronze.{table}")

    if cnt > 0:
        new_max = new_rows.agg(F.max(to_timestamp(col(wm_col)))).collect()[0][0]
        _update_watermark(spark, table, new_max)
        print(f"  watermark advanced to {new_max}")


def demo_time_travel(spark, table="orders"):
    """[demo] Prove the append-only incremental ingest keeps full history by
    listing the Iceberg snapshot history and reading an older snapshot with
    FOR VERSION AS OF."""
    full = f"bronze.{table}"
    if not spark.catalog.tableExists(full):
        return
    print(f"\n[time-travel demo] {full}.history")
    spark.sql(f"SELECT made_current_at, snapshot_id, is_current_ancestor "
              f"FROM {full}.history ORDER BY made_current_at").show(truncate=False)

    snaps = [r["snapshot_id"] for r in
             spark.sql(f"SELECT snapshot_id FROM {full}.snapshots "
                       f"ORDER BY committed_at").collect()]
    if snaps:
        first = snaps[0]
        current = spark.table(full).count()
        old = spark.sql(
            f"SELECT COUNT(*) AS c FROM {full} FOR VERSION AS OF {first}"
        ).collect()[0]["c"]
        print(f"  rows at first snapshot ({first}) = {old:,}")
        print(f"  rows at current snapshot           = {current:,}")


def main():
    spark = get_spark("bronze_ingest")
    ensure_databases(spark)

    with job_log(spark, "bronze", "bronze_ingest") as log:
        for csv_name, table in CSV_TO_TABLE.items():
            print(f"\n[bronze] {table}  (from {INGEST_SOURCE})")
            df = read_source(spark, table, csv_name)
            df = (df.withColumn("_ingested_at", current_timestamp())
                    .withColumn("_source_file", lit(csv_name)))

            if table in INCREMENTAL_TABLES:
                ingest_incremental(spark, table, df)
            else:
                partitioned_by = ""
                if table in BRONZE_PARTITIONED:
                    partitioned_by = "days(_ingested_at)"
                create_or_replace_iceberg(
                    df,
                    f"bronze.{table}",
                    partitioned_by=partitioned_by,
                )
                print(f"  wrote bronze.{table}: {df.count():,} rows")

        # showcase Iceberg time-travel on the incremental table
        for table in INCREMENTAL_TABLES:
            demo_time_travel(spark, table)

        log.rows_out = sum_counts(spark, [f"bronze.{t}" for t in CSV_TO_TABLE.values()])

    print("\nBronze ingest complete.")
    spark.stop()


if __name__ == "__main__":
    main()
