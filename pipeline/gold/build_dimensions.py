"""Silver -> Gold dimension tables (Star Schema). Write OVERWRITE to `gold`."""
import sys
sys.path.insert(0, "/opt/pipeline")

from pyspark.sql import functions as F, Window
from common.spark_session import get_spark, ensure_databases


def _write(df, table):
    (df.writeTo(f"gold.{table}").using("iceberg")
       .tableProperty("format-version", "2").createOrReplace())
    print(f"  wrote gold.{table}: {df.count():,} rows")


def dim_customer(spark):
    c = spark.table("silver.customers")
    # PK = customer_id (unique per order in Olist) -> keep every customer_id so
    # that fact_orders.customer_id never becomes an orphan FK. customer_unique_id
    # is kept as an attribute for retention analysis.
    c = c.dropDuplicates(["customer_id"])
    df = c.select("customer_id", "customer_unique_id",
                  F.col("customer_city").alias("city"),
                  F.col("customer_state").alias("state"),
                  F.col("customer_zip_code_prefix").alias("zip_code"))
    _write(df, "dim_customer")


def dim_product(spark):
    p = spark.table("silver.products")
    df = p.select(
        "product_id",
        F.col("product_category_name").alias("category_name"),
        F.col("product_category_name_english").alias("category_name_english"),
        F.col("product_weight_g").alias("weight_g"),
        F.col("product_length_cm").alias("length_cm"),
        F.col("product_height_cm").alias("height_cm"),
        F.col("product_width_cm").alias("width_cm"),
    )
    _write(df, "dim_product")


def dim_seller(spark):
    s = spark.table("silver.sellers")
    df = s.select("seller_id",
                  F.col("seller_city").alias("city"),
                  F.col("seller_state").alias("state"),
                  F.col("seller_zip_code_prefix").alias("zip_code"))
    _write(df, "dim_seller")


def dim_date(spark):
    # date spine 2016-01-01 .. 2019-12-31 (independent of data)
    df = spark.sql("""
        SELECT explode(sequence(to_date('2016-01-01'), to_date('2019-12-31'),
               interval 1 day)) AS full_date
    """)
    df = (df.withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int"))
            .withColumn("year", F.year("full_date"))
            .withColumn("quarter", F.quarter("full_date"))
            .withColumn("month", F.month("full_date"))
            .withColumn("day", F.dayofmonth("full_date"))
            .withColumn("day_of_week", F.dayofweek("full_date"))
            .withColumn("is_weekend", F.col("day_of_week").isin(1, 7)))
    df = df.select("date_id", "full_date", "year", "quarter", "month",
                   "day", "day_of_week", "is_weekend")
    _write(df, "dim_date")


def dim_payment_type(spark):
    p = spark.table("silver.order_payments")
    df = (p.select("payment_type").distinct()
           .filter(F.col("payment_type").isNotNull())
           .withColumn("payment_type_id",
                       F.row_number().over(Window.orderBy("payment_type"))))
    df = df.select("payment_type_id", "payment_type")
    _write(df, "dim_payment_type")


def main():
    spark = get_spark("gold_build_dimensions")
    ensure_databases(spark)
    dim_customer(spark)
    dim_product(spark)
    dim_seller(spark)
    dim_date(spark)
    dim_payment_type(spark)
    print("\nGold dimensions complete.")
    spark.stop()


if __name__ == "__main__":
    main()
