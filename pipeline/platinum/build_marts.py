"""[Extension #1] Gold -> Platinum business marts, pre-aggregated for Superset.
Write OVERWRITE to database `platinum`."""
import sys
sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import functions as F
from common.spark_session import get_spark, ensure_databases


def _write(df, table):
    (df.writeTo(f"platinum.{table}").using("iceberg")
       .tableProperty("format-version", "2").createOrReplace())
    print(f"  wrote platinum.{table}: {df.count():,} rows")


def mart_monthly_revenue(spark):
    fo = spark.table("gold.fact_orders")
    dd = spark.table("gold.dim_date")
    df = (fo.join(dd, fo.purchase_date_id == dd.date_id, "inner")
            .groupBy("year", "month")
            .agg(F.round(F.sum("total_revenue"), 2).alias("revenue"),
                 F.countDistinct("order_id").alias("order_count"))
            .orderBy("year", "month"))
    _write(df, "mart_monthly_revenue")


def mart_state_performance(spark):
    fo = spark.table("gold.fact_orders")
    dc = spark.table("gold.dim_customer")
    df = (fo.join(dc, "customer_id", "inner")
            .groupBy("state")
            .agg(F.countDistinct("order_id").alias("order_count"),
                 F.round(F.sum("total_revenue"), 2).alias("revenue"),
                 F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"))
            .orderBy(F.desc("order_count")))
    _write(df, "mart_state_performance")


def mart_category_ranking(spark):
    fi = spark.table("gold.fact_order_items")
    dp = spark.table("gold.dim_product")
    df = (fi.join(dp, "product_id", "inner")
            .groupBy("category_name_english")
            .agg(F.round(F.sum("price"), 2).alias("revenue"),
                 F.count("*").alias("items_sold"))
            .orderBy(F.desc("revenue")))
    _write(df, "mart_category_ranking")


def mart_delivery_kpi(spark):
    fo = spark.table("gold.fact_orders")
    df = fo.agg(
        F.count("*").alias("total_orders"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(F.sum(F.col("is_late_delivery").cast("int")) / F.count("*") * 100, 2)
         .alias("late_delivery_rate_pct"),
    )
    _write(df, "mart_delivery_kpi")


def mart_review_by_category(spark):
    fr = spark.table("gold.fact_reviews")
    fi = spark.table("gold.fact_order_items")
    dp = spark.table("gold.dim_product")
    df = (fr.join(fi, "order_id", "inner")
            .join(dp, "product_id", "inner")
            .groupBy("category_name_english")
            .agg(F.round(F.avg("review_score"), 2).alias("avg_review_score"),
                 F.count("*").alias("review_count"))
            .orderBy(F.desc("avg_review_score")))
    _write(df, "mart_review_by_category")


def mart_payment_distribution(spark):
    fp = spark.table("gold.fact_payments")
    df = (fp.groupBy("payment_type")
            .agg(F.count("*").alias("payment_count"),
                 F.round(F.sum("payment_value"), 2).alias("total_value"))
            .orderBy(F.desc("payment_count")))
    _write(df, "mart_payment_distribution")


def mart_customer_retention(spark):
    fo = spark.table("gold.fact_orders")
    dc = spark.table("gold.dim_customer")
    per_cust = (fo.join(dc, "customer_id", "inner")
                  .groupBy("customer_unique_id")
                  .agg(F.countDistinct("order_id").alias("order_count")))
    df = per_cust.agg(
        F.count("*").alias("total_customers"),
        F.sum(F.when(F.col("order_count") >= 2, 1).otherwise(0)).alias("repeat_customers"),
    ).withColumn(
        "retention_rate_pct",
        F.round(F.col("repeat_customers") / F.col("total_customers") * 100, 2),
    )
    _write(df, "mart_customer_retention")


def main():
    spark = get_spark("platinum_build_marts")
    ensure_databases(spark)
    mart_monthly_revenue(spark)
    mart_state_performance(spark)
    mart_category_ranking(spark)
    mart_delivery_kpi(spark)
    mart_review_by_category(spark)
    mart_payment_distribution(spark)
    mart_customer_retention(spark)
    print("\nPlatinum marts complete.")
    spark.stop()


if __name__ == "__main__":
    main()
