"""Silver -> Gold fact tables (Star Schema). Write OVERWRITE to `gold`."""
import sys
sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import functions as F
from common.spark_session import get_spark, ensure_databases
from common.job_log import job_log, sum_counts
from common.iceberg import create_or_replace_iceberg


def _write(df, table):
    create_or_replace_iceberg(df, f"gold.{table}")
    print(f"  wrote gold.{table}: {df.count():,} rows")


def fact_orders(spark):
    orders = spark.table("silver.orders")
    items = spark.table("silver.order_items")

    agg = items.groupBy("order_id").agg(
        F.count("*").alias("total_items"),
        F.sum("price").alias("total_revenue"),
        F.sum("freight_value").alias("freight_total"),
    )
    df = orders.join(agg, on="order_id", how="left")
    df = (df.withColumn("delivery_days",
                        F.datediff(F.col("order_delivered_customer_date"),
                                   F.col("order_purchase_timestamp")))
            .withColumn("is_late_delivery",
                        F.col("order_delivered_customer_date") >
                        F.col("order_estimated_delivery_date"))
            .withColumn("purchase_date_id",
                        F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"))
            .withColumn("delivered_date_id",
                        F.date_format("order_delivered_customer_date", "yyyyMMdd").cast("int")))
    df = df.select(
        "order_id", "customer_id", "order_status",
        "purchase_date_id", "delivered_date_id",
        F.coalesce("total_items", F.lit(0)).alias("total_items"),
        F.coalesce("total_revenue", F.lit(0.0)).alias("total_revenue"),
        F.coalesce("freight_total", F.lit(0.0)).alias("freight_total"),
        "order_duration_days", "delivery_days", "is_late_delivery",
    )
    _upsert_orders(spark, df)


def _upsert_orders(spark, df):
    """Idempotent upsert on order_id. First run creates the table; afterwards we
    MERGE so a redelivered/changed order (lifecycle replay) updates IN PLACE
    instead of being duplicated. MERGE has no delete-by-source clause, which is
    fine for an append-only replay where the source set only grows."""
    table = "gold.fact_orders"
    if not spark.catalog.tableExists(table):
        create_or_replace_iceberg(df, table)
        print(f"  created {table}: {df.count():,} rows")
        return
    df.createOrReplaceTempView("_fact_orders_src")
    spark.sql(f"""
        MERGE INTO {table} t
        USING _fact_orders_src s ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  merged {table}: now {spark.table(table).count():,} rows")


def fact_order_items(spark):
    items = spark.table("silver.order_items")
    df = items.select(
        "order_id", "order_item_id", "product_id", "seller_id",
        "price", "freight_value",
        F.lit(1).alias("quantity"),
    )
    _write(df, "fact_order_items")


def fact_reviews(spark):
    r = spark.table("silver.order_reviews")
    df = (r.withColumn("review_date_id",
                       F.date_format("review_creation_date", "yyyyMMdd").cast("int"))
           .withColumn("has_comment", F.col("review_comment_message").isNotNull())
           .select("review_id", "order_id", "review_date_id",
                   "review_score", "has_comment"))
    _write(df, "fact_reviews")


def fact_payments(spark):
    p = spark.table("silver.order_payments")
    # connect the fact to dim_payment_type via its surrogate key so the
    # dimension is no longer dangling (every payment_type -> payment_type_id).
    # build_dimensions runs before build_facts, so the dim already exists.
    dim = spark.table("gold.dim_payment_type")
    df = (p.join(dim, "payment_type", "left")
           .select("order_id", "payment_sequential",
                   "payment_type_id", "payment_type",
                   "payment_installments", "payment_value"))
    _write(df, "fact_payments")


def main():
    spark = get_spark("gold_build_facts")
    ensure_databases(spark)
    with job_log(spark, "gold", "gold_build_facts") as log:
        fact_orders(spark)
        fact_order_items(spark)
        fact_reviews(spark)
        fact_payments(spark)
        log.rows_out = sum_counts(spark, [f"gold.{t}" for t in
                                  ["fact_orders", "fact_order_items",
                                   "fact_reviews", "fact_payments"]])
    print("\nGold facts complete.")
    spark.stop()


if __name__ == "__main__":
    main()
