"""[Phase 1 + 2] Replay REAL Olist fact data into Postgres `olist_source`, one
calendar month per run, to give the lakehouse a "living" OLTP source for the
incremental-ingest + Iceberg time-travel (+ optional Gold upsert) demo.

No data is fabricated: every value comes from the Olist CSVs. We only release
the orders of one purchase-month at a time and (optionally) replay the order
lifecycle.

Two behaviours (env LIFECYCLE_MODE):
  0 (default, Phase 1) -> insert each order once, with its REAL final values;
                          source_updated_at = order_purchase_timestamp.
  1 (Phase 2)          -> CDC-style lifecycle: an order that the data says was
                          delivered in a LATER month than its purchase is first
                          inserted as 'shipped' (delivered date NULL), then a
                          later tick UPDATEs it to 'delivered' with the real
                          date + source_updated_at = order_delivered_customer_date.
                          Orders not delivered, delivered same-month, or with
                          anomalous dates (delivery < purchase) are inserted once
                          at their real final state (no 2nd version).
    -> Bronze then holds multiple versions of an order_id; Silver dedups to the
       latest source_updated_at and Gold MERGEs into fact_orders.

source_updated_at is always a BUSINESS-event time (deterministic / reproducible),
never wall-clock now(). In insert-only mode it equals order_purchase_timestamp,
so the watermark is identical to Phase 1.

Cursor of progress: Postgres `olist_source.sim_state(last_loaded_month)` (the
SOURCE-side replay pointer, distinct from lakehouse `meta.ingest_watermark`).

Algorithm (compute-target-from-state, advance-state-LAST, all in ONE txn):
  1. read last_loaded_month
  2. target = SIM_MONTH override | earliest order month (if none) | last + 1
  3. insert that month's orders (+ children by order_id) ON CONFLICT DO NOTHING
  4. [lifecycle] UPDATE orders delivered in target month (purchased earlier)
  5. advance sim_state = target

Pure python (pandas + sqlalchemy); no Spark."""
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
SIM_MONTH = os.environ.get("SIM_MONTH", "").strip()       # "YYYY-MM" override
LIFECYCLE = os.environ.get("LIFECYCLE_MODE", "0") == "1"  # Phase 2 toggle

ORDERS_CSV = "olist_orders_dataset.csv"
CHILD_CSVS = {
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
}
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
    """Idempotent insert via a staging table + INSERT ... ON CONFLICT DO NOTHING."""
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


def _update_delivered(conn, upd):
    """Reveal the real delivery on already-present orders (lifecycle). Joins by
    order_id so anomalous orders not yet inserted are simply skipped (no-op)."""
    if upd.empty:
        print("    delivered-updates: 0")
        return
    stg = "_stg_deliv"
    upd.to_sql(stg, conn, schema=SCHEMA, if_exists="replace", index=False)
    res = conn.execute(text(
        f"UPDATE {SCHEMA}.orders o "
        f"SET order_status='delivered', "
        f"    order_delivered_customer_date = s.delivered, "
        f"    source_updated_at = s.supd "
        f"FROM {SCHEMA}.{stg} s WHERE o.order_id = s.order_id"))
    conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{stg}"))
    print(f"    delivered-updates: {res.rowcount} order(s)")


def main():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@postgres:5432/{SOURCE_DB}"
    engine = create_engine(url)

    orders = pd.read_csv(os.path.join(DATASET_DIR, ORDERS_CSV))
    orders["_pm"] = pd.to_datetime(
        orders["order_purchase_timestamp"], errors="coerce").dt.to_period("M")
    orders["_dm"] = pd.to_datetime(
        orders["order_delivered_customer_date"], errors="coerce").dt.to_period("M")
    earliest, latest = orders["_pm"].min(), orders["_pm"].max()

    with engine.begin() as conn:  # one transaction: inserts + updates + state
        _ensure_state(conn)
        last = _read_state(conn)

        if SIM_MONTH:
            target = pd.Period(SIM_MONTH, "M")
        elif last is None:
            target = earliest
        else:
            target = pd.Period(last, "M") + 1

        print(f"[simulate_source] lifecycle={LIFECYCLE} last={last} "
              f"-> target={target} (data {earliest}..{latest})")

        # --- insert this month's orders (parents) ------------------------------
        mo = orders[orders["_pm"] == target].copy()
        mo["source_updated_at"] = mo["order_purchase_timestamp"]
        if LIFECYCLE:
            # hide delivery only for orders genuinely delivered in a LATER month
            mask = ((mo["order_status"] == "delivered")
                    & mo["_dm"].notna() & (mo["_dm"] > target))
            mo.loc[mask, "order_status"] = "shipped"
            mo.loc[mask, "order_delivered_customer_date"] = pd.NaT
            print(f"  orders in {target}: {len(mo)} "
                  f"({int(mask.sum())} masked as shipped)")
        else:
            print(f"  orders in {target}: {len(mo)}")
        oids = set(mo["order_id"])
        mo = mo.drop(columns=["_pm", "_dm"])
        _insert(conn, "orders", _coerce(mo, "orders"))

        # --- children of exactly those order_ids (FK-safe) ---------------------
        for table, csv in CHILD_CSVS.items():
            cdf = pd.read_csv(os.path.join(DATASET_DIR, csv))
            cdf = cdf[cdf["order_id"].isin(oids)]
            _insert(conn, table, _coerce(cdf, table))

        # --- lifecycle: reveal deliveries that land in this month --------------
        if LIFECYCLE:
            due = orders[(orders["order_status"] == "delivered")
                         & (orders["_dm"] == target)
                         & (orders["_pm"] < target)]
            upd = pd.DataFrame({
                "order_id": due["order_id"],
                "delivered": pd.to_datetime(due["order_delivered_customer_date"]),
                "supd": pd.to_datetime(due["order_delivered_customer_date"]),
            })
            _update_delivered(conn, upd)

        _set_state(conn, target.to_timestamp().date())
        print(f"  sim_state advanced to {target}")

    print("simulate_source done.")


if __name__ == "__main__":
    main()
