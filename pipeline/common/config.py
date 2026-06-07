"""Central configuration: source mappings, table lists, paths."""

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

# tables partitioned by ingest date in Bronze (large fact-like tables)
BRONZE_PARTITIONED = {"orders", "order_items", "order_payments", "order_reviews"}

# databases (namespaces) used across the lakehouse
DATABASES = ["bronze", "silver", "gold", "platinum", "meta"]

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

# Postgres source (extension #3)
PG_HOST = "postgres"
PG_PORT = 5432
PG_SOURCE_DB = "olist_source"
PG_SOURCE_SCHEMA = "olist_source"
PG_USER = "airflow"
PG_PASSWORD = "airflow"


def pg_jdbc_url(db: str = PG_SOURCE_DB) -> str:
    return f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{db}"


PG_PROPERTIES = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}
