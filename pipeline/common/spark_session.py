"""SparkSession factory + namespace bootstrap.

Most Iceberg / S3A / catalog config comes from spark-defaults.conf baked into
the images. Here we only set the app name and master (overridable via env so the
same job can run on the standalone cluster or in local mode for tests)."""
import os
from pyspark.sql import SparkSession

from .config import DATABASES


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
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_databases(spark: SparkSession) -> None:
    """Create all lakehouse namespaces if they do not exist yet."""
    for db in DATABASES:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
