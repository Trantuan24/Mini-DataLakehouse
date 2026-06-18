"""Gold tests: FK integrity, positive revenue, row-count consistency."""


def test_no_orphan_order_items(spark):
    items = spark.table("gold.fact_order_items")
    orders = spark.table("gold.fact_orders")
    orphans = items.join(orders.select("order_id").distinct(),
                         "order_id", "left_anti").count()
    assert orphans == 0


def test_fact_orders_revenue_non_negative(spark):
    fo = spark.table("gold.fact_orders")
    assert fo.filter(fo.total_revenue < 0).count() == 0


def test_fact_orders_matches_silver(spark):
    fo = spark.table("gold.fact_orders").count()
    so = spark.table("silver.orders").count()
    assert fo == so


def test_fact_orders_pk_unique(spark):
    fo = spark.table("gold.fact_orders")
    assert fo.count() == fo.select("order_id").distinct().count()


def test_dim_customer_pk_not_null(spark):
    dc = spark.table("gold.dim_customer")
    assert dc.filter(dc.customer_id.isNull()).count() == 0
