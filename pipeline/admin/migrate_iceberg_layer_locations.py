"""Rewrite existing Iceberg tables into the bucket that matches their layer.

This is a one-time repair for tables created before namespace LOCATIONs were
added. It rewrites the current snapshot only; old Iceberg snapshot history is
not preserved.

Safety default: without --apply this tool only prints a plan. Applying requires
explicit --tables so a broad database rewrite cannot happen by accident.
"""

import argparse
import sys
from datetime import datetime

sys.path.insert(0, "/opt/pipeline")

from common.config import DATABASE_LOCATIONS
from common.iceberg import create_or_replace_iceberg, sql_string, table_location
from common.spark_session import ensure_databases, get_spark


TEMP_DB = "migration_tmp"

PARTITION_SPECS = {
    "bronze.orders": "months(_part_ts)",
    "bronze.order_items": "days(_ingested_at)",
    "bronze.order_payments": "days(_ingested_at)",
    "bronze.order_reviews": "days(_ingested_at)",
}


def _table_location(spark, identifier):
    rows = spark.sql("DESCRIBE EXTENDED " + identifier).collect()
    for row in rows:
        key = str(row[0]).strip().lower()
        if key == "location":
            return str(row[1]).strip().rstrip("/")
    return ""


def _iter_tables(spark):
    for db in DATABASE_LOCATIONS:
        for table in spark.catalog.listTables(db):
            if table.name.startswith(("__migrate_", "__backup_")):
                continue
            yield db + "." + table.name


def _print_plan(spark, identifiers):
    planned = []
    for identifier in sorted(identifiers):
        current = _table_location(spark, identifier)
        target = table_location(identifier)
        source_count = spark.table(identifier).count()
        status = "ok" if current == target.rstrip("/") else "move"
        print("[{0}] {1}".format(status, identifier))
        print("      from: {0}".format(current or "(unknown)"))
        print("      to:   {0}".format(target))
        print("      rows: {0:,}".format(source_count))
        planned.append((identifier, current, target, source_count))
    return planned


def _rewrite_table(spark, source, temp, location):
    partition = PARTITION_SPECS.get(source)
    create_or_replace_iceberg(
        spark.table(source),
        temp,
        partitioned_by=partition or "",
        location=location,
    )


def migrate_table(spark, identifier, drop_backup=False):
    current = _table_location(spark, identifier)
    target = table_location(identifier)
    if current == target.rstrip("/"):
        print("[ok]   {0} already at {1}".format(identifier, target))
        return

    db, table = identifier.split(".", 1)
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    temp = "{0}.{1}__{2}__{3}".format(TEMP_DB, db, table, stamp)
    backup = "{0}.__backup_{1}_{2}".format(db, table, stamp)
    source_count = spark.table(identifier).count()

    spark.sql(
        "CREATE DATABASE IF NOT EXISTS {0} LOCATION {1}".format(
            TEMP_DB, sql_string("s3a://warehouse/migration_tmp/")
        )
    )
    spark.sql("DROP TABLE IF EXISTS " + temp)
    _rewrite_table(spark, identifier, temp, target)

    rewritten_count = spark.table(temp).count()
    if rewritten_count != source_count:
        spark.sql("DROP TABLE IF EXISTS " + temp)
        raise RuntimeError(
            "{0}: row count changed during migration ({1} -> {2})".format(
                identifier, source_count, rewritten_count
            )
        )

    spark.sql("ALTER TABLE {0} RENAME TO {1}".format(identifier, backup))
    spark.sql("ALTER TABLE {0} RENAME TO {1}".format(temp, identifier))

    final_count = spark.table(identifier).count()
    if final_count != source_count:
        raise RuntimeError(
            "{0}: final row count changed after rename ({1} -> {2})".format(
                identifier, source_count, final_count
            )
        )

    if drop_backup:
        spark.sql("DROP TABLE " + backup)
        print("[done] {0}; backup dropped".format(identifier))
    else:
        print("[done] {0}; backup kept at {1}".format(identifier, backup))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--apply",
        action="store_true",
        help="execute migration for the explicit --tables list",
    )
    parser.add_argument(
        "--drop-backup",
        action="store_true",
        help="drop table backups after migration succeeds",
    )
    parser.add_argument("--tables", nargs="*")
    args = parser.parse_args()

    spark = get_spark("migrate_iceberg_layer_locations")
    ensure_databases(spark)
    identifiers = args.tables or list(_iter_tables(spark))
    plan = _print_plan(spark, identifiers)

    if args.apply and not args.tables:
        spark.stop()
        raise SystemExit("--apply requires explicit --tables to avoid a broad rewrite")

    if args.apply:
        for identifier, current, target, _source_count in plan:
            if current != target.rstrip("/"):
                migrate_table(spark, identifier, drop_backup=args.drop_backup)

    spark.stop()


if __name__ == "__main__":
    main()
