"""Central configuration: source mappings, table lists, paths."""

import os

# CSV filename  ->  base table name (Build spec naming)
CSV_TO_TABLE = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation",
}

# tables partitioned by ingest date in Bronze (large fact-like tables).
# NOTE: `orders` is handled by the incremental path below (partitioned by the
# business month instead of the ingest day) so it is intentionally NOT here.
BRONZE_PARTITIONED = {"order_items", "order_payments", "order_reviews"}

# tables ingested incrementally (append + watermark) instead of full overwrite
INCREMENTAL_TABLES = {"orders"}

# high-watermark column = when the SOURCE ROW last changed. For orders this is
# `source_updated_at` (set to purchase time on insert, delivery time on the
# delivered-update) so that lifecycle UPDATEs are re-ingested as new versions.
# In insert-only / full mode source_updated_at == order_purchase_timestamp, so
# the watermark behaves exactly like before (backward compatible).
WATERMARK_COLUMN = {"orders": "source_updated_at"}

# partition column = business event-time (purchase month), kept separate from the
# watermark so versions of one order stay in their purchase-month partition.
PARTITION_COLUMN = {"orders": "order_purchase_timestamp"}

# Iceberg meta table holding the high-watermark per source table
WATERMARK_TABLE = "meta.ingest_watermark"

# databases (namespaces) used across the lakehouse. Each namespace gets its own
# object-storage root so newly created Iceberg tables land in the bucket that
# matches the medallion layer instead of the catalog-wide default warehouse.
DATABASE_LOCATIONS = {
    "bronze": os.environ.get("BRONZE_DB_LOCATION", "s3a://bronze/"),
    "silver": os.environ.get("SILVER_DB_LOCATION", "s3a://silver/"),
    "gold": os.environ.get("GOLD_DB_LOCATION", "s3a://gold/"),
    "platinum": os.environ.get("PLATINUM_DB_LOCATION", "s3a://platinum/"),
    "meta": os.environ.get("META_DB_LOCATION", "s3a://meta/"),
}
DATABASES = list(DATABASE_LOCATIONS)

# local mount where the dataset CSVs live (inside spark/airflow containers)
DATASET_DIR = "/opt/dataset"

# primary keys used for dedup / DQ
PRIMARY_KEYS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id"],
    "geolocation": ["geolocation_zip_code_prefix"],
    "category_translation": ["product_category_name"],
}

# Postgres source (extension #3). Defaults keep local Docker Compose easy to
# run, while env overrides avoid baking credentials into pipeline code.
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_SOURCE_DB = os.environ.get("SOURCE_DB", os.environ.get("PG_SOURCE_DB", "olist_source"))
PG_SOURCE_SCHEMA = os.environ.get("PG_SOURCE_SCHEMA", "olist_source")
PG_USER = os.environ.get("POSTGRES_USER", os.environ.get("PG_USER", "airflow"))
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", os.environ.get("PG_PASSWORD", "airflow"))


def pg_jdbc_url(db: str = PG_SOURCE_DB) -> str:
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{db}"


PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}
