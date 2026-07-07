"""Rewrite existing Iceberg tables into the bucket that matches their layer.

This is a one-time migration for tables that were created before namespace
LOCATIONs were added. It rewrites the current snapshot only; old Iceberg
snapshot history is not preserved.
"""

import argparse
import sys
from datetime import datetime

sys.path.insert(0, "/opt/pipeline")

from common.config import DATABASE_LOCATIONS
from common.spark_session import ensure_databases, get_spark


TEMP_DB = "migration_tmp"

PARTITION_SPECS = {
    "bronze.orders": "months(_part_ts)",
    "bronze.order_items": "days(_ingested_at)",
    "bronze.order_payments": "days(_ingested_at)",
    "bronze.order_reviews": "days(_ingested_at)",
}


def _sql_string(value):
    return "'" + value.replace("'", "''") + "'"


def _target_location(identifier):
    db, table = identifier.split(".", 1)
    return DATABASE_LOCATIONS[db].rstrip("/") + "/" + table


def _table_location(spark, identifier):
    rows = spark.sql("DESCRIBE EXTENDED " + identifier).collect()
    for row in rows:
        key = str(row[0]).strip().lower()
        if key == "location":
            return str(row[1]).strip().rstrip("/")
    return ""


def _tables_by_db(spark):
    found = {}
    for db in DATABASE_LOCATIONS:
        found[db] = []
        for table in spark.catalog.listTables(db):
            if table.name.startswith("__migrate_") or table.name.startswith("__backup_"):
                continue
            found[db].append(table.name)
        found[db].sort()
    return found


def _temp_identifier(db, table, stamp):
    return "{0}.{1}__{2}__{3}".format(TEMP_DB, db, table, stamp)


def _rewrite_table(spark, source, temp, location):
    partition = PARTITION_SPECS.get(source)
    partition_sql = "PARTITIONED BY (" + partition + ")" if partition else ""
    spark.table(source).createOrReplaceTempView("__migration_source")
    spark.sql("""
        CREATE TABLE {temp}
        USING iceberg
        {partition_sql}
        LOCATION {location}
        TBLPROPERTIES ('format-version' = '2')
        AS SELECT * FROM __migration_source
    """.format(
        temp=temp,
        partition_sql=partition_sql,
        location=_sql_string(location),
    ))


def _recreate_database(spark, db):
    location = DATABASE_LOCATIONS[db].rstrip("/") + "/"
    spark.sql("DROP DATABASE IF EXISTS " + db)
    spark.sql(
        "CREATE DATABASE {0} LOCATION {1}".format(db, _sql_string(location))
    )


def migrate_table(spark, identifier, dry_run=False):
    """Migrate one table location.

    This mode is useful for targeted repair, but it cannot recreate the parent
    database location while other tables still exist. The default full migration
    below is better for fixing old deployments end to end.
    """
    current = _table_location(spark, identifier)
    target = _target_location(identifier)
    if current == target.rstrip("/"):
        print("[ok]   {0} already at {1}".format(identifier, target))
        return

    db, table = identifier.split(".", 1)
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    spark.sql(
        "CREATE DATABASE IF NOT EXISTS {0} LOCATION {1}".format(
            TEMP_DB, _sql_string("s3a://warehouse/migration_tmp/")
        )
    )
    temp = _temp_identifier(db, table, stamp)
    source_count = spark.table(identifier).count()

    print("[move] {0}".format(identifier))
    print("       from: {0}".format(current or "(unknown)"))
    print("       to:   {0}".format(target))
    print("       rows: {0:,}".format(source_count))
    if dry_run:
        return

    spark.sql("DROP TABLE IF EXISTS " + temp)
    _rewrite_table(spark, identifier, temp, target)

    target_count = spark.table(temp).count()
    if target_count != source_count:
        spark.sql("DROP TABLE IF EXISTS " + temp)
        raise RuntimeError(
            "{0}: row count changed during migration ({1} -> {2})".format(
                identifier, source_count, target_count
            )
        )

    backup = "{0}.__backup_{1}_{2}".format(db, table, stamp)
    spark.sql("ALTER TABLE {0} RENAME TO {1}".format(identifier, backup))
    spark.sql("ALTER TABLE {0} RENAME TO {1}".format(temp, identifier))

    final_count = spark.table(identifier).count()
    if final_count != source_count:
        raise RuntimeError(
            "{0}: final row count changed after rename ({1} -> {2})".format(
                identifier, source_count, final_count
            )
        )

    spark.sql("DROP TABLE " + backup)
    print("[done] {0}; old table dropped".format(identifier))


def migrate_all(spark, dry_run=False):
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    tables_by_db = _tables_by_db(spark)

    spark.sql(
        "CREATE DATABASE IF NOT EXISTS {0} LOCATION {1}".format(
            TEMP_DB, _sql_string("s3a://warehouse/migration_tmp/")
        )
    )

    plan = []
    for db, tables in tables_by_db.items():
        for table in tables:
            identifier = db + "." + table
            current = _table_location(spark, identifier)
            target = _target_location(identifier)
            source_count = spark.table(identifier).count()
            plan.append((db, table, identifier, current, target, source_count))
            status = "ok" if current == target.rstrip("/") else "move"
            print("[{0}] {1}".format(status, identifier))
            print("      from: {0}".format(current or "(unknown)"))
            print("      to:   {0}".format(target))
            print("      rows: {0:,}".format(source_count))

    if dry_run:
        return

    temp_tables = {}
    for db, table, identifier, _current, target, source_count in plan:
        temp = _temp_identifier(db, table, stamp)
        spark.sql("DROP TABLE IF EXISTS " + temp)
        _rewrite_table(spark, identifier, temp, target)
        target_count = spark.table(temp).count()
        if target_count != source_count:
            raise RuntimeError(
                "{0}: row count changed during rewrite ({1} -> {2})".format(
                    identifier, source_count, target_count
                )
            )
        temp_tables[identifier] = temp

    for db, tables in tables_by_db.items():
        for table in tables:
            spark.sql("DROP TABLE " + db + "." + table)
        _recreate_database(spark, db)
        for table in tables:
            identifier = db + "." + table
            spark.sql(
                "ALTER TABLE {0} RENAME TO {1}".format(
                    temp_tables[identifier], identifier
                )
            )

    for db, table, identifier, _current, _target, source_count in plan:
        final_count = spark.table(identifier).count()
        if final_count != source_count:
            raise RuntimeError(
                "{0}: final row count changed ({1} -> {2})".format(
                    identifier, source_count, final_count
                )
            )
        print("[done] {0}: {1:,} rows".format(identifier, final_count))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--tables", nargs="*")
    args = parser.parse_args()

    spark = get_spark("migrate_iceberg_layer_locations")
    ensure_databases(spark)
    if args.tables:
        for identifier in args.tables:
            migrate_table(spark, identifier, dry_run=args.dry_run)
    else:
        migrate_all(spark, dry_run=args.dry_run)
    spark.stop()


if __name__ == "__main__":
    main()
