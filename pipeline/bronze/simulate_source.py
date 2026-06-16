"""[Phase 1] Replay REAL Olist fact data into Postgres `olist_source`, one
calendar month per run, to give the lakehouse a "living" OLTP source for the
incremental-ingest + Iceberg time-travel demo.

No data is fabricated: every row comes straight from the Olist CSVs. We just
release the orders of one purchase-month (plus their order_items / payments /
reviews) at a time. Pair this with `load_source.py SEED_MODE=dims_only` (which
seeds the dimensions once and leaves the 4 fact tables empty).

Cursor of progress lives in Postgres `olist_source.sim_state(last_loaded_month)`
-- this is the SOURCE-side replay pointer, distinct from the lakehouse-side
`meta.ingest_watermark` (different concern, different layer).

Algorithm (compute-target-from-state, advance-state-LAST -> retry safe):
  1. read last_loaded_month
  2. target = SIM_MONTH override, else earliest order month (if none loaded),
     else last_loaded_month + 1 month
  3. insert that month's orders + their children (ON CONFLICT DO NOTHING)
  4. advance sim_state = target  -- all of 2-4 in ONE transaction, so a crash
     rolls back cleanly and a retry recomputes the same target.

Pure python (pandas + sqlalchemy + psycopg2); no Spark."""
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, "/opt/pipeline")
from bronze.load_source import _coerce  # reuse the exact type coercion

DATASET_DIR = os.environ.get("DATASET_DIR", "/opt/dataset")
PG_USER = os.environ.get("POSTGRES_USER", "airflow")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")
SOURCE_DB = os.environ.get("SOURCE_DB", "olist_source")
SCHEMA = "olist_source"
# optional override to jump to a specific month, e.g. "2017-03" (demo/debug)
SIM_MONTH = os.environ.get("SIM_MONTH", "").strip()

ORDERS_CSV = "olist_orders_dataset.csv"
CHILD_CSVS = {
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
}
# ON CONFLICT target = the table's PRIMARY KEY (matches load_source DDL)
PK = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id", "order_id"],
}


def _ensure_state(conn):
    conn.execute(text(
        f"CREATE TABLE IF NOT EXISTS {SCHEMA}.sim_state "
        f"(id int PRIMARY KEY, last_loaded_month date)"))
    conn.execute(text(
        f"INSERT INTO {SCHEMA}.sim_state (id, last_loaded_month) "
        f"VALUES (1, NULL) ON CONFLICT (id) DO NOTHING"))


def _read_state(conn):
    r = conn.execute(text(
        f"SELECT last_loaded_month FROM {SCHEMA}.sim_state WHERE id=1")).fetchone()
    return r[0] if r else None


def _set_state(conn, month_first_day):
    conn.execute(text(
        f"UPDATE {SCHEMA}.sim_state SET last_loaded_month=:d WHERE id=1"),
        {"d": month_first_day})


def _insert(conn, table, df):
    """Idempotent insert via a staging table + INSERT ... ON CONFLICT DO NOTHING.
    Staging lets pandas/sqlalchemy handle all type adaptation; the ON CONFLICT
    keeps re-runs (or a same-month override) from duplicating rows."""
    cols = list(df.columns)
    if df.empty:
        print(f"    {table}: 0 rows")
        return
    stg = f"_stg_{table}"
    df.to_sql(stg, conn, schema=SCHEMA, if_exists="replace", index=False)
    collist = ", ".join(cols)
    pk = ", ".join(PK[table])
    res = conn.execute(text(
        f"INSERT INTO {SCHEMA}.{table} ({collist}) "
        f"SELECT {collist} FROM {SCHEMA}.{stg} "
        f"ON CONFLICT ({pk}) DO NOTHING"))
    conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{stg}"))
    print(f"    {table}: +{res.rowcount} new (of {len(df)} in month)")


def main():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@postgres:5432/{SOURCE_DB}"
    engine = create_engine(url)

    orders = pd.read_csv(os.path.join(DATASET_DIR, ORDERS_CSV))
    orders["_pm"] = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce").dt.to_period("M")
    earliest, latest = orders["_pm"].min(), orders["_pm"].max()

    with engine.begin() as conn:  # one transaction: inserts + state advance
        _ensure_state(conn)
        last = _read_state(conn)

        if SIM_MONTH:
            target = pd.Period(SIM_MONTH, "M")
        elif last is None:
            target = earliest
        else:
            target = pd.Period(last, "M") + 1

        print(f"[simulate_source] last_loaded={last} -> target={target} "
              f"(data range {earliest}..{latest})")

        month_orders = orders[orders["_pm"] == target].drop(columns=["_pm"])
        oids = set(month_orders["order_id"])
        print(f"  orders in {target}: {len(month_orders)}")

        if month_orders.empty:
            print(f"  (no orders for {target} - empty month / past end of data; "
                  f"advancing state anyway)")

        # parents first, then children of exactly those order_ids (FK-safe)
        _insert(conn, "orders", _coerce(month_orders, "orders"))
        for table, csv in CHILD_CSVS.items():
            cdf = pd.read_csv(os.path.join(DATASET_DIR, csv))
            cdf = cdf[cdf["order_id"].isin(oids)]
            _insert(conn, table, _coerce(cdf, table))

        # advance the cursor LAST, inside the same transaction
        _set_state(conn, target.to_timestamp().date())
        print(f"  sim_state advanced to {target}")

    print("simulate_source done.")


if __name__ == "__main__":
    main()
