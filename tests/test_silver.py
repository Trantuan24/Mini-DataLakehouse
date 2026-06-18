"""Silver tests: no null PKs, no duplicate PKs."""
import pytest


def test_silver_orders_pk_not_null(spark):
    df = spark.table("silver.orders")
    assert df.filter(df.order_id.isNull()).count() == 0


def test_silver_orders_pk_unique(spark):
    df = spark.table("silver.orders")
    assert df.count() == df.select("order_id").distinct().count()


def test_silver_order_items_composite_pk_unique(spark):
    df = spark.table("silver.order_items")
    assert df.count() == df.select("order_id", "order_item_id").distinct().count()


def test_silver_review_score_range(spark):
    df = spark.table("silver.order_reviews")
    bad = df.filter((df.review_score < 1) | (df.review_score > 5)).count()
    assert bad == 0


def test_silver_order_status_accepted_values(spark):
    df = spark.table("silver.orders")
    allowed = [
        "created", "approved", "invoiced", "processing", "shipped",
        "delivered", "canceled", "unavailable",
    ]
    bad = df.filter(df.order_status.isNotNull() & ~df.order_status.isin(*allowed)).count()
    assert bad == 0
