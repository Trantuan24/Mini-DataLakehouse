"""Data quality gate on Silver tables."""
import sys
sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import functions as F
from common.spark_session import get_spark
from common import data_quality as dq


def main():
    spark = get_spark("silver_validate")
    results = []

    orders = spark.table("silver.orders")
    results.append(dq.expect_column_not_null(orders, "order_id", table="silver.orders", layer="silver"))
    results.append(dq.expect_unique(orders, ["order_id"], table="silver.orders", layer="silver"))
    results.append(dq.expect_column_values_in_set(
        orders, "order_status",
        {"created", "approved", "invoiced", "processing", "shipped", "delivered",
         "canceled", "unavailable"},
        table="silver.orders", layer="silver"))

    items = spark.table("silver.order_items")
    results.append(dq.expect_unique(items, ["order_id", "order_item_id"], table="silver.order_items", layer="silver"))
    results.append(dq.expect_column_positive(items, "price", table="silver.order_items", layer="silver"))

    payments = spark.table("silver.order_payments")
    results.append(dq.expect_column_values_in_set(
        payments, "payment_type",
        {"credit_card", "boleto", "voucher", "debit_card", "not_defined"},
        table="silver.order_payments", layer="silver"))

    reviews = spark.table("silver.order_reviews")
    results.append(dq.expect_column_between(reviews, "review_score", 1, 5, table="silver.order_reviews", layer="silver"))

    # timestamp validity window
    bad_ts = orders.filter(
        (F.col("order_purchase_timestamp") < F.lit("2016-01-01")) |
        (F.col("order_purchase_timestamp") > F.lit("2019-12-31"))
    ).count()
    results.append(dq._result("silver.orders", "silver", "purchase_ts_in_range", bad_ts == 0, bad_ts, 0))

    dq.run_suite(spark, "silver", results)
    spark.stop()


if __name__ == "__main__":
    main()
