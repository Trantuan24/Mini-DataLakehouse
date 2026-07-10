"""Reset every Iceberg layer for a fresh full-load or replay scenario.

The command deliberately leaves the MinIO ``raw`` bucket and local CSV dataset
untouched.  It removes only Iceberg tables and their layer prefixes, including
``meta.ingest_watermark``.  Old snapshot history is therefore not retained.

Run through ``scripts/run_mode.sh`` rather than invoking this module directly.
Without ``--apply`` it prints the reset plan only.
"""
import argparse
import sys

sys.path.insert(0, "/opt/pipeline")

from common.config import DATABASE_LOCATIONS
from common.spark_session import ensure_databases, get_spark


def database_location(spark, database):
    rows = spark.sql("DESCRIBE DATABASE EXTENDED " + database).collect()
    for row in rows:
        if str(row[0]).strip().lower() == "location":
            return str(row[1]).strip().rstrip("/") + "/"
    return ""


def list_tables(spark, database):
    return sorted(
        database + "." + row.tableName
        for row in spark.sql("SHOW TABLES IN " + database).collect()
        if not row.isTemporary
    )


def delete_layer_prefixes(spark, locations):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    for location in sorted(locations):
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(location)
        filesystem = path.getFileSystem(hadoop_conf)
        if filesystem.exists(path):
            # S3A refuses to delete a bucket root (s3a://bronze/), so clear
            # its children one by one while preserving the bucket itself.
            if path.toUri().getPath() in (None, "", "/"):
                for child in filesystem.listStatus(path):
                    filesystem.delete(child.getPath(), True)
            else:
                filesystem.delete(path, True)
            print("[deleted] " + location)
        else:
            print("[absent]  " + location)


def main():
    parser = argparse.ArgumentParser(
        description="Remove Iceberg layer state for a clean scenario."
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="perform the reset; omit for a read-only plan",
    )
    args = parser.parse_args()

    spark = get_spark("reset_lakehouse_state")
    try:
        databases = []
        storage_locations = set(DATABASE_LOCATIONS.values())
        for database in DATABASE_LOCATIONS:
            current_location = database_location(spark, database)
            if current_location:
                storage_locations.add(current_location)
            databases.append((database, list_tables(spark, database)))

        print("Iceberg tables to remove:")
        for _, tables in databases:
            for table in tables:
                print("  - " + table)
        print("Storage prefixes to remove:")
        for location in sorted(storage_locations):
            print("  - " + location)

        if not args.apply:
            print("Dry run only. Re-run with --apply to reset the state.")
            return

        for database, _ in databases:
            spark.sql("DROP DATABASE IF EXISTS " + database + " CASCADE")
            print("[dropped]  namespace " + database)

        delete_layer_prefixes(spark, storage_locations)
        ensure_databases(spark)
        print("Lakehouse state reset complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
