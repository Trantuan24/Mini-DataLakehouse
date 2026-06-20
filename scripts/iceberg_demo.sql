-- =====================================================================
-- Iceberg Features Demo - run in Trino (catalog: iceberg)
--
-- Option A: open Trino CLI and paste blocks manually
--   docker compose exec -T trino trino --catalog iceberg
--
-- Option B: copy this file into the Trino container and run safe queries
--   docker compose cp scripts/iceberg_demo.sql trino:/tmp/iceberg_demo.sql
--   docker compose exec -T trino trino --catalog iceberg -f /tmp/iceberg_demo.sql
--
-- Note: FOR VERSION AS OF needs a concrete snapshot_id. The manual examples
-- are commented out below so this file can run end-to-end without failing.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Snapshot history: each ingest/replay creates a new Iceberg snapshot.
-- ---------------------------------------------------------------------
SELECT snapshot_id, parent_id, operation, committed_at
FROM iceberg.bronze."orders$snapshots"
ORDER BY committed_at;

SELECT made_current_at, snapshot_id, is_current_ancestor
FROM iceberg.bronze."orders$history"
ORDER BY made_current_at;

-- Snapshot candidates for manual time-travel.
SELECT snapshot_id, committed_at, operation
FROM iceberg.bronze."orders$snapshots"
ORDER BY committed_at
LIMIT 5;

SELECT snapshot_id, committed_at, operation
FROM iceberg.bronze."orders$snapshots"
ORDER BY committed_at DESC
LIMIT 5;

-- ---------------------------------------------------------------------
-- 2. Time-travel examples.
-- Copy snapshot_id values from the queries above, then run these manually.
-- Do not keep the angle brackets; use the numeric snapshot id directly.
-- ---------------------------------------------------------------------
-- SELECT count(*) AS rows_at_old_snapshot
-- FROM iceberg.bronze.orders FOR VERSION AS OF 123456789;

-- SELECT count(*) AS rows_at_new_snapshot
-- FROM iceberg.bronze.orders FOR VERSION AS OF 987654321;

-- Replace the timestamp with a committed_at value from orders$snapshots.
-- SELECT count(*) AS rows_at_timestamp
-- FROM iceberg.bronze.orders
-- FOR TIMESTAMP AS OF TIMESTAMP '2026-06-18 00:00:00 UTC';

-- ---------------------------------------------------------------------
-- 3. MERGE INTO evidence on gold.fact_orders.
-- Same order_id lifecycle updates must not create duplicate fact rows.
-- ---------------------------------------------------------------------
SELECT count(*) AS total_rows,
       count(DISTINCT order_id) AS distinct_orders,
       count(*) - count(DISTINCT order_id) AS duplicates
FROM iceberg.gold.fact_orders;

SELECT order_status, count(*) AS cnt
FROM iceberg.gold.fact_orders
GROUP BY order_status
ORDER BY cnt DESC;

SELECT snapshot_id, operation, committed_at
FROM iceberg.gold."fact_orders$snapshots"
ORDER BY committed_at;

-- ---------------------------------------------------------------------
-- 4. Physical data-file metadata.
-- ---------------------------------------------------------------------
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.bronze."orders$files"
ORDER BY record_count DESC
LIMIT 20;
