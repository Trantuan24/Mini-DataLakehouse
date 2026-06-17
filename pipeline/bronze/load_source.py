"""[Extension #3] Load raw Olist CSV files into Postgres `olist_source` schema
to simulate an OLTP source system. Bronze can then ingest from this DB via JDBC.

Unlike a quick `to_sql(if_exists="replace")` dump, this creates each table with
an explicit DDL + PRIMARY KEY (proper OLTP shape) and then *appends* the rows.
Tables are dropped/recreated first so the seed job stays idempotent.

Two modes (env SEED_MODE):
  full       (default) -> seed ALL 9 tables (the Step-0 baseline; pipeline can
                          run straight after this).
  dims_only  -> seed only the 5 reference/dimension tables; the 4 fact-like
                tables (orders/order_items/order_payments/order_reviews) are
                CREATED EMPTY and owned by simulate_source.py, which replays
                them month by month (Phase 1 incremental/time-travel demo).

Run inside a container that has pandas + sqlalchemy + psycopg2 (the airflow image).
This job does NOT need Spark."""
import os
import sys
import glob

import pandas as pd
from sqlalchemy import create_engine, text

DATASET_DIR = os.environ.get("DATASET_DIR", "/opt/dataset")
PG_USER = os.environ.get("POSTGRES_USER", "airflow")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")
SOURCE_DB = os.environ.get("SOURCE_DB", "olist_source")
SCHEMA = "olist_source"
SEED_MODE = os.environ.get("SEED_MODE", "full")  # full | dims_only

# fact-like tables replayed over time by simulate_source.py (left empty in
# dims_only mode); the rest are reference/dimension tables seeded once.
FACT_TABLES = {"orders", "order_items", "order_payments", "order_reviews"}

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

# Explicit DDL with PRIMARY KEY for every table (OLTP-like source schema).
# `geolocation` keeps a surrogate serial PK (its natural key, the zip prefix,
# repeats many times). `order_reviews` uses the composite (review_id, order_id)
# which IS unique in the raw dump (review_id alone is not) -> preserves every
# row AND gives the replay simulator a real ON CONFLICT target.
DDL = {
    "orders": """
        CREATE TABLE {s}.orders (
            order_id varchar(32) PRIMARY KEY,
            customer_id varchar(32) NOT NULL,
            order_status varchar(20),
            order_purchase_timestamp timestamp,
            order_approved_at timestamp,
            order_delivered_carrier_date timestamp,
            order_delivered_customer_date timestamp,
            order_estimated_delivery_date timestamp,
            source_updated_at timestamp
        )""",
    "order_items": """
        CREATE TABLE {s}.order_items (
            order_id varchar(32) NOT NULL,
            order_item_id integer NOT NULL,
            product_id varchar(32),
            seller_id varchar(32),
            shipping_limit_date timestamp,
            price numeric(12,2),
            freight_value numeric(12,2),
            PRIMARY KEY (order_id, order_item_id)
        )""",
    "customers": """
        CREATE TABLE {s}.customers (
            customer_id varchar(32) PRIMARY KEY,
            customer_unique_id varchar(32) NOT NULL,
            customer_zip_code_prefix integer,
            customer_city varchar(64),
            customer_state varchar(4)
        )""",
    "products": """
        CREATE TABLE {s}.products (
            product_id varchar(32) PRIMARY KEY,
            product_category_name varchar(64),
            product_name_lenght integer,
            product_description_lenght integer,
            product_photos_qty integer,
            product_weight_g integer,
            product_length_cm integer,
            product_height_cm integer,
            product_width_cm integer
        )""",
    "sellers": """
        CREATE TABLE {s}.sellers (
            seller_id varchar(32) PRIMARY KEY,
            seller_zip_code_prefix integer,
            seller_city varchar(64),
            seller_state varchar(4)
        )""",
    "order_payments": """
        CREATE TABLE {s}.order_payments (
            order_id varchar(32) NOT NULL,
            payment_sequential integer NOT NULL,
            payment_type varchar(20),
            payment_installments integer,
            payment_value numeric(12,2),
            PRIMARY KEY (order_id, payment_sequential)
        )""",
    "order_reviews": """
        CREATE TABLE {s}.order_reviews (
            review_id varchar(32) NOT NULL,
            order_id varchar(32) NOT NULL,
            review_score integer,
            review_comment_title varchar(128),
            review_comment_message text,
            review_creation_date timestamp,
            review_answer_timestamp timestamp,
            PRIMARY KEY (review_id, order_id)
        )""",
    "geolocation": """
        CREATE TABLE {s}.geolocation (
            geolocation_id bigserial PRIMARY KEY,
            geolocation_zip_code_prefix integer,
            geolocation_lat double precision,
            geolocation_lng double precision,
            geolocation_city varchar(64),
            geolocation_state varchar(4)
        )""",
    "category_translation": """
        CREATE TABLE {s}.category_translation (
            product_category_name varchar(64) PRIMARY KEY,
            product_category_name_english varchar(64)
        )""",
}

# column type coercion so the appended values match the explicit DDL types
TS_COLS = {
    "orders": ["order_purchase_timestamp", "order_approved_at",
               "order_delivered_carrier_date", "order_delivered_customer_date",
               "order_estimated_delivery_date", "source_updated_at"],
    "order_items": ["shipping_limit_date"],
    "order_reviews": ["review_creation_date", "review_answer_timestamp"],
}
INT_COLS = {
    "order_items": ["order_item_id"],
    "customers": ["customer_zip_code_prefix"],
    "products": ["product_name_lenght", "product_description_lenght",
                 "product_photos_qty", "product_weight_g", "product_length_cm",
                 "product_height_cm", "product_width_cm"],
    "sellers": ["seller_zip_code_prefix"],
    "order_payments": ["payment_sequential", "payment_installments"],
    "order_reviews": ["review_score"],
    "geolocation": ["geolocation_zip_code_prefix"],
}
NUMERIC_COLS = {
    "order_items": ["price", "freight_value"],
    "order_payments": ["payment_value"],
    "geolocation": ["geolocation_lat", "geolocation_lng"],
}


def _coerce(df, table):
    for c in TS_COLS.get(table, []):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    for c in INT_COLS.get(table, []):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in NUMERIC_COLS.get(table, []):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def main():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@postgres:5432/{SOURCE_DB}"
    engine = create_engine(url)

    found = glob.glob(os.path.join(DATASET_DIR, "*.csv"))
    if not found:
        print(f"ERROR: no CSV files found in {DATASET_DIR}. "
              f"Place the Olist dataset there first.", file=sys.stderr)
        sys.exit(1)

    print(f"[load_source] SEED_MODE={SEED_MODE}")
    loaded = 0
    for csv_name, table in CSV_TO_TABLE.items():
        path = os.path.join(DATASET_DIR, csv_name)
        if not os.path.exists(path):
            print(f"  (skip) missing {csv_name}")
            continue

        # always (re)create the schema with explicit DDL + PK
        print(f"  creating {SCHEMA}.{table} (explicit DDL + PK) ...")
        with engine.begin() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{table} CASCADE"))
            conn.execute(text(DDL[table].format(s=SCHEMA)))

        # in dims_only mode leave the fact-like tables EMPTY for simulate_source
        if SEED_MODE == "dims_only" and table in FACT_TABLES:
            print(f"    (dims_only) left empty -> owned by simulate_source")
            continue

        print(f"  appending {csv_name} -> {SCHEMA}.{table} ...")
        df = pd.read_csv(path)
        if table == "orders":
            # full / insert-only load has one version per order, so the row's
            # last-change time is just its creation time.
            df["source_updated_at"] = df["order_purchase_timestamp"]
        df = _coerce(df, table)
        df.to_sql(table, engine, schema=SCHEMA, if_exists="append",
                  index=False, chunksize=10000, method="multi")
        print(f"    {len(df):,} rows")
        loaded += 1

    print(f"Done ({SEED_MODE}): populated {loaded} tables in {SOURCE_DB}.{SCHEMA}")


if __name__ == "__main__":
    main()
