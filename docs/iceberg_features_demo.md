# Iceberg Features Demo — Time-travel, Snapshot History & MERGE INTO

Tài liệu này minh hoạ **vì sao dự án chọn Apache Iceberg** chứ không chỉ lưu Parquet thuần:
khả năng **versioning (snapshot)**, **time-travel** và **upsert bằng MERGE INTO** (CDC).

Toàn bộ truy vấn chạy trong **Trino** (catalog `iceberg`). File SQL đầy đủ:
[`scripts/iceberg_demo.sql`](../scripts/iceberg_demo.sql).

> Cách chạy nhanh:
> ```bash
> docker compose exec -T trino trino --catalog iceberg
> ```
> rồi dán từng câu lệnh bên dưới. Bảng minh hoạ chính là `bronze.orders`
> (được `simulate_source` replay theo từng tháng nên sinh nhiều snapshot) và
> `gold.fact_orders` (dùng MERGE INTO để upsert vòng đời đơn hàng).

---

## 1. Snapshot history — mỗi lần ghi tạo một phiên bản mới

Iceberg ghi **bất biến (immutable)**: mỗi lần ingest/replay/MERGE tạo một *snapshot*
mới thay vì sửa đè. Bảng metadata `…$snapshots` và `…$history` cho ta xem toàn bộ
lịch sử commit.

```sql
SELECT snapshot_id, parent_id, operation, committed_at
FROM iceberg.bronze."orders$snapshots"
ORDER BY committed_at;
```

```sql
SELECT made_current_at, snapshot_id, is_current_ancestor
FROM iceberg.bronze."orders$history"
ORDER BY made_current_at;
```

**Kết quả mong đợi:** nhiều dòng snapshot, `operation` gồm `append` (ingest thêm
tháng) và có thể `overwrite`; thời điểm `committed_at` tăng dần theo các lần replay.

📷 **Ảnh cần chụp:** kết quả 2 câu trên trong Trino CLI / Trino Web UI (port 8090).
Lưu vào: `docs/images/iceberg/01_snapshots.png`

<!-- ![Snapshot history](images/iceberg/01_snapshots.png) -->

---

## 2. Time-travel — đọc lại trạng thái quá khứ

Lấy 2 `snapshot_id` từ mục 1 (một cũ, một mới), thay vào `FOR VERSION AS OF`:

```sql
-- snapshot CŨ (vd sau tháng đầu replay)
SELECT count(*) AS rows_at_old_snapshot
FROM iceberg.bronze.orders FOR VERSION AS OF <SNAP_CU>;

-- snapshot MỚI (sau nhiều tháng) -> số dòng lớn hơn
SELECT count(*) AS rows_at_new_snapshot
FROM iceberg.bronze.orders FOR VERSION AS OF <SNAP_MOI>;
```

Cũng có thể time-travel theo **mốc thời gian**:

```sql
SELECT count(*) AS rows_at_timestamp
FROM iceberg.bronze.orders
FOR TIMESTAMP AS OF TIMESTAMP '2026-06-18 00:00:00 UTC';
```

**Kết quả mong đợi:** số dòng ở snapshot mới > snapshot cũ — chứng minh dữ liệu lớn
dần qua các lần replay và mọi phiên bản cũ vẫn đọc được (audit / reproducibility).

> Đối chiếu Phase 1 đã verify: `bronze.orders` tăng dần qua các snapshot
> (ví dụ 4 → 328 dòng giữa 2 mốc replay).

📷 **Ảnh cần chụp:** 2 kết quả count khác nhau ở 2 snapshot.
Lưu vào: `docs/images/iceberg/02_time_travel.png`

<!-- ![Time travel](images/iceberg/02_time_travel.png) -->

---

## 3. MERGE INTO — upsert vòng đời đơn hàng (CDC)

`gold.fact_orders` được xây bằng `MERGE INTO` thay vì ghi đè:

```sql
MERGE INTO gold.fact_orders t
USING <silver_orders_latest> s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Khi một đơn chuyển trạng thái (`shipped` → `delivered`), bản ghi cũ **được cập nhật
tại chỗ về mặt logic** (Iceberg tạo snapshot mới), **không** sinh dòng trùng `order_id`.

**Kiểm chứng không trùng PK:**

```sql
SELECT count(*) AS total_rows,
       count(DISTINCT order_id) AS distinct_orders,
       count(*) - count(DISTINCT order_id) AS duplicates
FROM iceberg.gold.fact_orders;
```

→ `duplicates = 0`.

**Phân bố trạng thái sau MERGE** (cho thấy lifecycle UPDATE đã được áp dụng):

```sql
SELECT order_status, count(*) AS cnt
FROM iceberg.gold.fact_orders
GROUP BY order_status
ORDER BY cnt DESC;
```

> Đối chiếu Phase 2 đã verify: 444 đơn `shipped` được MERGE lật thành `delivered`,
> tổng số dòng không đổi (2580), `duplicates = 0`, rerun idempotent.

📷 **Ảnh cần chụp:**
- `docs/images/iceberg/03a_merge_no_dup.png` — kết quả `duplicates = 0`.
- `docs/images/iceberg/03b_status_after_merge.png` — phân bố trạng thái.

<!-- ![No duplicates after MERGE](images/iceberg/03a_merge_no_dup.png) -->
<!-- ![Status after MERGE](images/iceberg/03b_status_after_merge.png) -->

---

## 4. Tóm tắt — vì sao Iceberg

| Tính năng | Câu lệnh minh hoạ | Lợi ích cho Lakehouse |
|---|---|---|
| Snapshot/versioning | `…$snapshots`, `…$history` | Audit, lineage, rollback |
| Time-travel | `FOR VERSION/TIMESTAMP AS OF` | Reproducibility, debug dữ liệu quá khứ |
| MERGE INTO (upsert) | `WHEN MATCHED/NOT MATCHED` | CDC, idempotent, không trùng PK |
| Schema/format-version 2 | `tableProperty('format-version','2')` | Row-level delete/update hiệu quả |

---

## Phụ lục — thư mục ảnh

Đặt screenshot vào `docs/images/iceberg/` theo tên gợi ý ở trên, rồi **bỏ comment**
(`<!-- ... -->`) ở các dòng `![...]` tương ứng để ảnh hiển thị khi xem trên GitHub
hoặc khi chèn vào báo cáo.
