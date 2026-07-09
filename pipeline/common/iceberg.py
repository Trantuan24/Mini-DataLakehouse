"""Small helpers for Iceberg table locations."""

from uuid import uuid4

from pyspark.sql import functions as F

from .config import DATABASE_LOCATIONS


def table_location(identifier: str) -> str:
    db, table = identifier.split(".", 1)
    return DATABASE_LOCATIONS[db].rstrip("/") + "/" + table


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def new_snapshot_uuid() -> str:
    return str(uuid4())


def append_iceberg(df, identifier: str) -> str:
    """Append to an Iceberg table and stamp a project-level UUID in the summary."""
    snapshot_uuid = new_snapshot_uuid()
    (df.writeTo(identifier)
       .option("snapshot-property.snapshot_uuid", snapshot_uuid)
       .append())
    return snapshot_uuid


def create_or_replace_iceberg(
    df,
    identifier: str,
    partitioned_by: str = "",
    location: str = "",
) -> None:
    """Create/replace an Iceberg table with snapshot_uuid in the commit summary."""
    snapshot_uuid = new_snapshot_uuid()
    writer = (df.writeTo(identifier)
                .using("iceberg")
                .option("path", location or table_location(identifier))
                .option("snapshot-property.snapshot_uuid", snapshot_uuid)
                .tableProperty("format-version", "2"))
    if partitioned_by:
        writer = writer.partitionedBy(F.expr(partitioned_by))
    writer.createOrReplace()
