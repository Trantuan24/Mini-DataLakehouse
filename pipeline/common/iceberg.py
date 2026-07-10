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


def partition_exprs(partitioned_by: str):
    expressions = []
    transforms = {
        "days": F.days,
        "hours": F.hours,
        "months": F.months,
        "years": F.years,
    }
    for raw_expr in partitioned_by.split(","):
        expr = raw_expr.strip()
        matched = False
        for name, fn in transforms.items():
            prefix = name + "("
            if expr.startswith(prefix) and expr.endswith(")"):
                col_name = expr[len(prefix):-1].strip()
                expressions.append(fn(col_name))
                matched = True
                break
        if not matched and expr:
            expressions.append(F.col(expr))
    return expressions


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
    """Create/replace an Iceberg table with pinned location and snapshot_uuid."""
    snapshot_uuid = new_snapshot_uuid()
    writer = (
        df.writeTo(identifier)
        .using("iceberg")
        .tableProperty("location", location or table_location(identifier))
        .tableProperty("format-version", "2")
        .option("snapshot-property.snapshot_uuid", snapshot_uuid)
    )
    if partitioned_by:
        writer = writer.partitionedBy(*partition_exprs(partitioned_by))
    writer.createOrReplace()
