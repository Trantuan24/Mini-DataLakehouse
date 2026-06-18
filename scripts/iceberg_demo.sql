-- =====================================================================
-- Iceberg Features Demo - chạy trong Trino (catalog: iceberg)
-- Cách chạy:
--   docker compose exec -T trino trino --catalog iceberg
--   rồi dán từng câu, hoặc:
--   docker compose exec -T trino trino --catalog iceberg -f /tmp/iceberg_demo.sql
-- Bảng minh hoạ: bronze.orders (được replay nhiều tháng -> nhiều snapshot).
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. SNAPSHOT HISTORY: mỗi lần ingest/replay tạo 1 snapshot mới
-- ---------------------------------------------------------------------
SELECT snapshot_id, parent_id, operation, committed_at
FROM iceberg.bronze."orders$snapshots"
ORDER BY committed_at;

-- Lịch sử commit (ánh xạ snapshot theo thời gian)
SELECT made_current_at, snapshot_id, is_current_ancestor
FROM iceberg.bronze."orders$history"
ORDER BY made_current_at;

-- ---------------------------------------------------------------------
-- 2. TIME TRAVEL: đọc lại trạng thái bảng ở quá khứ
--    Thay <SNAP_CU> / <SNAP_MOI> bằng snapshot_id lấy từ câu (1).
-- ---------------------------------------------------------------------
-- Số dòng ở snapshot CŨ (vd sau tháng đầu replay)
SELECT count(*) AS rows_at_old_snapshot
FROM iceberg.bronze.orders FOR VERSION AS OF <SNAP_CU>;

-- Số dòng ở snapshot MỚI (sau nhiều tháng) -> phải lớn hơn
SELECT count(*) AS rows_at_new_snapshot
FROM iceberg.bronze.orders FOR VERSION AS OF <SNAP_MOI>;

-- Time-travel theo MỐC THỜI GIAN (thay timestamp phù hợp)
SELECT count(*) AS rows_at_timestamp
FROM iceberg.bronze.orders
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-18 00:00:00 UTC';

-- ---------------------------------------------------------------------
-- 3. MERGE INTO (CDC upsert) - minh chứng ở gold.fact_orders
--    Cùng order_id chuyển shipped -> delivered KHÔNG tạo dòng trùng.
-- ---------------------------------------------------------------------
-- 3a. Không có order_id trùng (PK uniqueness sau MERGE)
SELECT count(*) AS total_rows,
       count(DISTINCT order_id) AS distinct_orders,
       count(*) - count(DISTINCT order_id) AS duplicates
FROM iceberg.gold.fact_orders;

-- 3b. Phân bố trạng thái sau khi lifecycle UPDATE được MERGE vào
SELECT order_status, count(*) AS cnt
FROM iceberg.gold.fact_orders
GROUP BY order_status
ORDER BY cnt DESC;

-- 3c. (Tuỳ chọn) số snapshot của fact_orders: mỗi lần MERGE = 1 snapshot
SELECT count(*) AS snapshot_count
FROM iceberg.gold."fact_orders$snapshots";

-- ---------------------------------------------------------------------
-- 4. (Tuỳ chọn) Chi tiết file dữ liệu của 1 bảng (data layout)
-- ---------------------------------------------------------------------
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.bronze."orders$files"
LIMIT 20;
