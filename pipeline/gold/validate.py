"""Data quality gate on Gold Star Schema (FK integrity, measures, PKs)."""
import sys
sys.path.insert(0, "/opt/pipeline")

from common.spark_session import get_spark
from common import data_quality as dq


def main():
    spark = get_spark("gold_validate")
    results = []

    fact_orders = spark.table("gold.fact_orders")
    fact_items = spark.table("gold.fact_order_items")
    dim_customer = spark.table("gold.dim_customer")
    dim_product = spark.table("gold.dim_product")
    dim_seller = spark.table("gold.dim_seller")

    # no orphan FKs
    results.append(dq.expect_no_orphans(
        fact_items, "order_id", fact_orders, "order_id",
        table="gold.fact_order_items", layer="gold"))
    results.append(dq.expect_no_orphans(
        fact_orders, "customer_id", dim_customer, "customer_id",
        table="gold.fact_orders", layer="gold"))
    results.append(dq.expect_no_orphans(
        fact_items, "product_id", dim_product, "product_id",
        table="gold.fact_order_items", layer="gold"))
    results.append(dq.expect_no_orphans(
        fact_items, "seller_id", dim_seller, "seller_id",
        table="gold.fact_order_items", layer="gold"))

    # measures / PKs
    results.append(dq.expect_column_not_null(dim_customer, "customer_id", table="gold.dim_customer", layer="gold"))
    results.append(dq.expect_column_not_null(dim_product, "product_id", table="gold.dim_product", layer="gold"))

    # fact_orders row count matches silver.orders
    fo = fact_orders.count()
    so = spark.table("silver.orders").count()
    results.append(dq._result("gold.fact_orders", "gold", "rowcount_matches_silver_orders", fo == so, fo, so))

    dq.run_suite(spark, "gold", results)
    spark.stop()


if __name__ == "__main__":
    main()
