"""Bronze -> Silver: clean, cast, dedupe, enrich. Write OVERWRITE to `silver`."""
import sys
sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import functions as F, Window
from pyspark.sql.types import DoubleType, IntegerType, TimestampType
from common.spark_session import get_spark, ensure_databases
from common.job_log import job_log, sum_counts
from common.iceberg import create_or_replace_iceberg


def _trim_strings(df):
    for f in df.schema.fields:
        if f.dataType.simpleString() == "string":
            df = df.withColumn(f.name, F.trim(F.col(f.name)))
            df = df.withColumn(
                f.name,
                F.when(F.col(f.name) == "", None).otherwise(F.col(f.name)),
            )
    return df


def _write(df, table):
    create_or_replace_iceberg(df, f"silver.{table}")
    print(f"  wrote silver.{table}: {df.count():,} rows")


def silver_orders(spark):
    df = _trim_strings(spark.table("bronze.orders"))
    ts_cols = ["order_purchase_timestamp", "order_approved_at",
               "order_delivered_carrier_date", "order_delivered_customer_date",
               "order_estimated_delivery_date", "source_updated_at"]
    for c in ts_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(TimestampType()))
    df = df.filter(F.col("order_id").isNotNull())

    # Bronze is append-only and (under lifecycle replay) holds multiple versions
    # of the same order_id. Keep the LATEST version per order_id: newest
    # source_updated_at wins, _ingested_at as a deterministic tiebreak. For
    # insert-only / full data (one version per order) this is a no-op.
    order_key = ["source_updated_at"]
    if "_ingested_at" in df.columns:
        order_key.append("_ingested_at")
    w = Window.partitionBy("order_id").orderBy(
        *[F.col(c).desc_nulls_last() for c in order_key])
    df = (df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1).drop("_rn"))

    df = df.withColumn(
        "order_duration_days",
        F.datediff(F.col("order_delivered_customer_date"), F.col("order_purchase_timestamp")),
    )
    _write(df, "orders")


def silver_order_items(spark):
    df = _trim_strings(spark.table("bronze.order_items"))
    df = (df.withColumn("price", F.col("price").cast(DoubleType()))
            .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
            .withColumn("shipping_limit_date", F.col("shipping_limit_date").cast(TimestampType()))
            .withColumn("order_item_id", F.col("order_item_id").cast(IntegerType())))
    df = df.dropDuplicates(["order_id", "order_item_id"])
    _write(df, "order_items")


def silver_customers(spark):
    df = _trim_strings(spark.table("bronze.customers"))
    df = df.withColumn("customer_state", F.upper(F.col("customer_state")))
    df = df.dropDuplicates(["customer_id"])
    _write(df, "customers")


def silver_products(spark):
    products = _trim_strings(spark.table("bronze.products"))
    # keep only the join key + translation; dropping category_translation's own
    # _ingested_at/_source_file metadata avoids a COLUMN_ALREADY_EXISTS clash
    # with the same columns already on `products`.
    trans = (_trim_strings(spark.table("bronze.category_translation"))
             .select("product_category_name", "product_category_name_english"))
    df = products.join(trans, on="product_category_name", how="left")
    df = df.withColumn(
        "product_category_name_english",
        F.coalesce(F.col("product_category_name_english"), F.lit("unknown")),
    )
    df = df.dropDuplicates(["product_id"])
    _write(df, "products")


def silver_sellers(spark):
    df = _trim_strings(spark.table("bronze.sellers"))
    df = df.withColumn("seller_state", F.upper(F.col("seller_state")))
    df = df.dropDuplicates(["seller_id"])
    _write(df, "sellers")


def silver_order_payments(spark):
    df = _trim_strings(spark.table("bronze.order_payments"))
    df = (df.withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
            .withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
            .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType())))
    # total payment value per order (kept as extra column)
    totals = df.groupBy("order_id").agg(F.sum("payment_value").alias("total_payment_value"))
    df = df.join(totals, on="order_id", how="left")
    df = df.dropDuplicates(["order_id", "payment_sequential"])
    _write(df, "order_payments")


def silver_order_reviews(spark):
    df = _trim_strings(spark.table("bronze.order_reviews"))
    df = (df.withColumn("review_score", F.col("review_score").cast(IntegerType()))
            .withColumn("review_creation_date", F.col("review_creation_date").cast(TimestampType()))
            .withColumn("review_answer_timestamp", F.col("review_answer_timestamp").cast(TimestampType())))
    df = df.dropDuplicates(["review_id"])
    _write(df, "order_reviews")


def silver_geolocation(spark):
    df = _trim_strings(spark.table("bronze.geolocation"))
    df = (df.withColumn("geolocation_lat", F.col("geolocation_lat").cast(DoubleType()))
            .withColumn("geolocation_lng", F.col("geolocation_lng").cast(DoubleType())))
    df = df.filter(
        (F.col("geolocation_lat").between(-35, 6)) &
        (F.col("geolocation_lng").between(-75, -34))
    )
    df = df.dropDuplicates(["geolocation_zip_code_prefix"])
    _write(df, "geolocation")


SILVER_TABLES = ["orders", "order_items", "customers", "products", "sellers",
                 "order_payments", "order_reviews", "geolocation"]


def main():
    spark = get_spark("silver_transform")
    ensure_databases(spark)
    with job_log(spark, "silver", "silver_transform") as log:
        silver_orders(spark)
        silver_order_items(spark)
        silver_customers(spark)
        silver_products(spark)
        silver_sellers(spark)
        silver_order_payments(spark)
        silver_order_reviews(spark)
        silver_geolocation(spark)
        log.rows_out = sum_counts(spark, [f"silver.{t}" for t in SILVER_TABLES])
    print("\nSilver transform complete.")
    spark.stop()


if __name__ == "__main__":
    main()
