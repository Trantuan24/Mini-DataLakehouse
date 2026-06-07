"""Silver -> Gold fact tables (Star Schema). Write OVERWRITE to `gold`."""
import sys
sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import functions as F
from common.spark_session import get_spark, ensure_databases


def _write(df, table):
    (df.writeTo(f"gold.{table}").using("iceberg")
       .tableProperty("format-version", "2").createOrReplace())
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
    _write(df, "fact_orders")


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
    df = p.select("order_id", "payment_sequential", "payment_type",
                  "payment_installments", "payment_value")
    _write(df, "fact_payments")


def main():
    spark = get_spark("gold_build_facts")
    ensure_databases(spark)
    fact_orders(spark)
    fact_order_items(spark)
    fact_reviews(spark)
    fact_payments(spark)
    print("\nGold facts complete.")
    spark.stop()


if __name__ == "__main__":
    main()
