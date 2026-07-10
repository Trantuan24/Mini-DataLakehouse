"""SparkSession factory + namespace bootstrap.

Most Iceberg / S3A / catalog config comes from spark-defaults.conf baked into
the images. Here we only set the app name and master (overridable via env so the
same job can run on the standalone cluster or in local mode for tests)."""
import os
from typing import Optional

from pyspark.sql import SparkSession

from .config import DATABASE_LOCATIONS


def get_spark(app_name: str = "lakehouse-job") -> SparkSession:
    master = os.environ.get("SPARK_MASTER_URL", "local[*]")
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # ensure config present even if spark-defaults.conf is missing (e.g. tests)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
        )
        .config("spark.sql.catalog.spark_catalog.type", "hive")
    )
    if master.startswith("local"):
        builder = (
            builder
            .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "localhost"))
            .config("spark.driver.bindAddress", os.environ.get("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1"))
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _normalize_location(location: str) -> str:
    return location.rstrip("/") + "/"


def _database_location(spark: SparkSession, db: str) -> Optional[str]:
    rows = spark.sql(f"DESCRIBE DATABASE EXTENDED {db}").collect()
    for row in rows:
        item = str(row[0]).strip().lower()
        if item == "location":
            return _normalize_location(str(row[1]).strip())
    return None


def ensure_databases(spark: SparkSession) -> None:
    """Create all lakehouse namespaces and keep their default locations aligned.

    Namespace LOCATION only affects tables created after the change. Existing
    Iceberg tables keep their own paths until they are rebuilt/migrated."""
    for db, raw_location in DATABASE_LOCATIONS.items():
        location = _normalize_location(raw_location)
        sql_location = _sql_string(location)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db} LOCATION {sql_location}")

        current = _database_location(spark, db)
        if current != location:
            try:
                spark.sql(f"ALTER DATABASE {db} SET LOCATION {sql_location}")
                print(f"[spark_session] aligned {db} location -> {location}")
            except Exception as e:
                print(
                    f"[spark_session] warning: {db} location is {current}, "
                    f"wanted {location}; Hive metastore cannot alter existing "
                    f"database locations ({e})"
                )
