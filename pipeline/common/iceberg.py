"""Small helpers for Iceberg table locations."""

from uuid import uuid4

from .config import DATABASE_LOCATIONS


def table_location(identifier: str) -> str:
    db, table = identifier.split(".", 1)
    return DATABASE_LOCATIONS[db].rstrip("/") + "/" + table


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def schema_sql(df) -> str:
    cols = [
        f"{sql_identifier(field.name)} {field.dataType.simpleString()}"
        for field in df.schema.fields
    ]
    return ",\n                ".join(cols)


def new_snapshot_uuid() -> str:
    return str(uuid4())


def append_iceberg(df, identifier: str) -> str:
    """Append to an Iceberg table and stamp a project-level UUID in the summary."""
    snapshot_uuid = new_snapshot_uuid()
    (df.writeTo(identifier)
       .option("snapshot-property.snapshot_uuid", snapshot_uuid)
       .append())
    return snapshot_uuid


def sql_with_snapshot_uuid(spark, query: str) -> str:
    """Run a Spark SQL write while stamping snapshot_uuid into Iceberg summary."""
    snapshot_uuid = new_snapshot_uuid()
    props = None
    try:
        props = spark._jvm.org.apache.iceberg.spark.CommitMetadata.commitProperties()
        props.put("snapshot_uuid", snapshot_uuid)
        spark.sql(query)
    finally:
        if props is not None:
            props.remove("snapshot_uuid")
    return snapshot_uuid


def create_or_replace_iceberg(df, identifier: str, partitioned_by: str = "") -> None:
    """Create/replace an Iceberg table while pinning its physical LOCATION."""
    spark = df.sparkSession
    partition_sql = f"PARTITIONED BY ({partitioned_by})" if partitioned_by else ""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {identifier} (
            {schema_sql(df)}
        )
        USING iceberg
        {partition_sql}
        LOCATION {sql_string(table_location(identifier))}
        TBLPROPERTIES ('format-version' = '2')
    """)
    append_iceberg(df, identifier)
