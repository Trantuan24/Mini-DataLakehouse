"""Small helpers for Iceberg table locations."""

from uuid import uuid4

from .config import DATABASE_LOCATIONS


def table_location(identifier: str) -> str:
    db, table = identifier.split(".", 1)
    return DATABASE_LOCATIONS[db].rstrip("/") + "/" + table


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def create_or_replace_iceberg(df, identifier: str, partitioned_by: str = "") -> None:
    """Create/replace an Iceberg table while pinning its physical LOCATION."""
    view = "__iceberg_write_" + uuid4().hex
    spark = df.sparkSession
    df.createOrReplaceTempView(view)
    partition_sql = f"PARTITIONED BY ({partitioned_by})" if partitioned_by else ""
    try:
        spark.sql(f"""
            CREATE OR REPLACE TABLE {identifier}
            USING iceberg
            {partition_sql}
            LOCATION {sql_string(table_location(identifier))}
            TBLPROPERTIES ('format-version' = '2')
            AS SELECT * FROM {view}
        """)
    finally:
        spark.catalog.dropTempView(view)
