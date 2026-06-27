"""[Extension #1] Gold -> Platinum business marts, pre-aggregated for Superset.
Write OVERWRITE to database `platinum`."""
import sys
sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import functions as F, Window
from common.spark_session import get_spark, ensure_databases
from common.job_log import job_log, sum_counts

# orders in these statuses never produced revenue -> excluded from $ marts
NON_REVENUE_STATUSES = ["canceled", "unavailable"]


def _write(df, table):
    (df.writeTo(f"platinum.{table}").using("iceberg")
       .tableProperty("format-version", "2").createOrReplace())
    print(f"  wrote platinum.{table}: {df.count():,} rows")


def mart_monthly_revenue(spark):
    fo = (spark.table("gold.fact_orders")
               .filter(~F.col("order_status").isin(NON_REVENUE_STATUSES)))
    dd = spark.table("gold.dim_date")
    df = (fo.join(dd, fo.purchase_date_id == dd.date_id, "inner")
            .groupBy("year", "month")
            .agg(F.round(F.sum("total_revenue"), 2).alias("revenue"),
                 F.countDistinct("order_id").alias("order_count"))
            .orderBy("year", "month"))
    _write(df, "mart_monthly_revenue")


def mart_state_performance(spark):
    # exclude canceled/unavailable so revenue here matches mart_monthly_revenue
    # (same business definition of "revenue" across every dashboard).
    fo = (spark.table("gold.fact_orders")
               .filter(~F.col("order_status").isin(NON_REVENUE_STATUSES)))
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
    # exclude items belonging to canceled/unavailable orders from revenue
    valid_orders = (spark.table("gold.fact_orders")
                         .filter(~F.col("order_status").isin(NON_REVENUE_STATUSES))
                         .select("order_id"))
    df = (fi.join(valid_orders, "order_id", "inner")
            .join(dp, "product_id", "inner")
            .groupBy("category_name_english")
            .agg(F.round(F.sum("price"), 2).alias("revenue"),
                 F.count("*").alias("items_sold"))
            .orderBy(F.desc("revenue")))
    _write(df, "mart_category_ranking")


def mart_delivery_kpi(spark):
    fo = spark.table("gold.fact_orders")
    # late_delivery_rate is only defined over orders that were actually
    # delivered; undelivered orders must not dilute the denominator.
    is_delivered = ((F.col("order_status") == "delivered")
                    | F.col("delivery_days").isNotNull())
    delivered_cnt = F.sum(is_delivered.cast("int"))
    late_cnt = F.sum(F.when(is_delivered & F.col("is_late_delivery"), 1).otherwise(0))
    df = fo.agg(
        F.count("*").alias("total_orders"),
        F.round(F.avg("delivery_days"), 2).alias("avg_delivery_days"),
        F.round(late_cnt / delivered_cnt * 100, 2).alias("late_delivery_rate_pct"),
    )
    _write(df, "mart_delivery_kpi")


def mart_review_by_category(spark):
    fr = spark.table("gold.fact_reviews")
    fi = spark.table("gold.fact_order_items")
    dp = spark.table("gold.dim_product")

    # A review belongs to an order, but an order can span many items across
    # several categories. Joining reviews x items directly fans each review out
    # once per item (wrong grain), inflating review_count and skewing the avg.
    # Fix: collapse every order to a single representative category first (the
    # dominant category by item count, deterministic tiebreak), THEN join the
    # review so each review contributes to exactly one category.
    items_cat = (fi.join(dp, "product_id", "inner")
                   .groupBy("order_id", "category_name_english")
                   .agg(F.count("*").alias("item_cnt")))
    rank = Window.partitionBy("order_id").orderBy(
        F.desc("item_cnt"), F.asc("category_name_english"))
    order_cat = (items_cat.withColumn("rn", F.row_number().over(rank))
                          .filter(F.col("rn") == 1)
                          .select("order_id", "category_name_english"))

    df = (fr.join(order_cat, "order_id", "inner")
            .groupBy("category_name_english")
            .agg(F.round(F.avg("review_score"), 2).alias("avg_review_score"),
                 F.countDistinct("review_id").alias("review_count"))
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
    with job_log(spark, "platinum", "platinum_build_marts") as log:
        mart_monthly_revenue(spark)
        mart_state_performance(spark)
        mart_category_ranking(spark)
        mart_delivery_kpi(spark)
        mart_review_by_category(spark)
        mart_payment_distribution(spark)
        mart_customer_retention(spark)
        log.rows_out = sum_counts(spark, [f"platinum.{t}" for t in
                                  ["mart_monthly_revenue", "mart_state_performance",
                                   "mart_category_ranking", "mart_delivery_kpi",
                                   "mart_review_by_category", "mart_payment_distribution",
                                   "mart_customer_retention"]])
    print("\nPlatinum marts complete.")
    spark.stop()


if __name__ == "__main__":
    main()
