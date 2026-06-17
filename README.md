# Mini Data Platform — Lakehouse (Olist Brazil)

Mini Lakehouse end-to-end:
**Postgres → Bronze → Silver → Gold → Platinum → Superset**, điều phối bằng Airflow,
lưu trữ Iceberg trên MinIO, truy vấn bằng Trino.


## Kiến trúc

```
CSV ─▶ Postgres(olist_source) ─▶ Bronze ─▶ Silver ─▶ Gold ─▶ Platinum ─▶ Superset
                                  └────── Iceberg trên MinIO ──────┘        (qua Trino)
        Airflow điều phối · DQ framework tự viết (kiểu GE, DQ gate) · pytest (tests/)
```

## Tech stack
Airflow 2.9 · Spark 3.5 (PySpark) · Apache Iceberg 1.5 · Hive Metastore 4.0 ·
MinIO · Trino 440 · Superset 3.1 · PostgreSQL 14 · Docker Compose.

## Yêu cầu
- Docker + Docker Compose (24+), RAM khuyến nghị >= 8GB.
- Tải dataset Olist từ Kaggle và đặt 9 file CSV trực tiếp vào `dataset/`.

## Chạy hệ thống

```bash
# 1. Build images (lần đầu tải Spark + jars, hơi lâu)
docker compose build

# 2. Khởi động toàn bộ services
docker compose up -d

# 3. (tùy chọn) upload CSV lên MinIO raw bucket
python scripts/upload_raw_data.py
```

| Service | URL | Login |
|---------|-----|-------|
| MinIO Console | http://localhost:9001 | admin / password |
| Airflow | http://localhost:8085 | admin / admin |
| Trino | http://localhost:8090 | (no auth) |
| Superset | http://localhost:8088 | admin / admin |
| Spark Master | http://localhost:8080 | — |

## Chạy pipeline
Có **3 DAG** (đều manual trigger), tách nguồn khỏi pipeline phân tích — pipeline
không bao giờ mutate nguồn:

1. **`seed_source_postgres`** — nạp Olist vào Postgres `olist_source` (DDL tường minh
   + PRIMARY KEY). Conf `{"mode":"full"}` (mặc định, nạp đủ 9 bảng) hoặc
   `{"mode":"dims_only"}` (chỉ seed 5 dim, để 4 bảng fact rỗng cho replay).
2. **`simulate_source`** — *(tùy chọn)* phát lại Olist **theo tháng** vào nguồn để
   demo incremental/time-travel/upsert. Conf `{"month":"2017-03"}` (nhảy tháng) và
   `{"lifecycle":"1"}` (replay vòng đời CDC: insert chưa-giao → update delivered).
3. **`lakehouse_pipeline`** — pipeline phân tích, gom task theo tầng medallion
   (TaskGroup `bronze`/`silver`/`gold`/`platinum`):

```
bronze[ingest → validate] → silver[transform → validate]
→ gold[dims → facts → validate] → platinum[marts]
→ run_etl_tests → notify_done
```

> Bronze ingest `orders` theo **incremental** watermark (`meta.ingest_watermark`),
> append, partition theo `months(order_purchase_timestamp)`. Watermark chạy trên
> `source_updated_at` (= purchase khi insert, = ngày giao khi update) nên các thay
> đổi vòng đời được tái-ingest thành version mới. Silver dedup lấy bản mới nhất mỗi
> `order_id`; Gold `fact_orders` dùng **`MERGE INTO`** (upsert) — re-run idempotent.

### Demo replay (incremental → time-travel → upsert)
```
seed_source_postgres  conf {"mode":"dims_only"}      # 1 lần
simulate_source       conf {"lifecycle":"1"}          # +1 tháng
lakehouse_pipeline    conf {"replay":"1"}             # ingest → … → platinum
# lặp 2 bước cuối: bronze.orders tăng dần; FOR VERSION AS OF cho số khác nhau
```

## Kiểm tra kết quả (qua Trino)
```sql
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;
SELECT COUNT(*) FROM iceberg.gold.fact_orders;
SELECT * FROM iceberg.platinum.mart_monthly_revenue ORDER BY year, month;
```

## Cấu trúc thư mục
```
├── docker-compose.yml          # 9 services
├── docker/                     # images & config hạ tầng
│   ├── airflow/  spark/  hive/  postgres/  trino/  superset/
├── pipeline/                   # source code ETL theo tầng
│   ├── common/                 # config, spark_session, data_quality, job_log
│   ├── bronze/                 # load_source, simulate_source, ingest, validate
│   ├── silver/                 # transform, validate
│   ├── gold/                   # build_dimensions, build_facts, validate
│   └── platinum/               # build_marts
├── dags/                       # lakehouse_pipeline, seed_source, simulate_source
├── notebooks/                  # eda_olist.ipynb + figures/ (EDA cho báo cáo)
├── tests/                      # pytest (bronze/silver/gold)
├── scripts/                    # init buckets, upload raw
└── dataset/                    # 9 CSV Olist (đặt trực tiếp ở đây)
```

## Phân tích khám phá (EDA)
`notebooks/eda_olist.ipynb` đọc trực tiếp `dataset/*.csv` (read-only, tách khỏi
stack) — tổng quan, profiling null, phân phối, insight nghiệp vụ, vấn đề DQ. Xuất
biểu đồ ra `notebooks/figures/`. Chạy: `pip install jupyterlab && jupyter lab`.

## Ghi chú thiết kế
- **Iceberg warehouse**: tất cả bảng nằm dưới bucket `warehouse` (`s3a://warehouse/<db>.db/`),
  Trino đọc qua catalog `iceberg` + Hive Metastore. Các bucket `bronze/silver/gold/platinum`
  vẫn được tạo cho dữ liệu raw/metadata và minh hoạ kiến trúc phân tầng.
- **Idempotent**: hầu hết Bronze/Silver/Gold/Platinum dùng `createOrReplace` (overwrite) →
  chạy lại pipeline cho kết quả giống nhau. Riêng `bronze.orders` là **incremental append**
  theo watermark (`meta.ingest_watermark`) nên chạy lại không tạo bản ghi trùng, đồng thời
  vẫn idempotent (không có dòng mới ⇒ không append).
- **Upsert (Gold)**: `gold.fact_orders` dùng `MERGE INTO` theo `order_id` (WHEN MATCHED
  UPDATE / NOT MATCHED INSERT) → replay vòng đời cập nhật đơn tại chỗ, không nhân bản.
- **DQ gate**: mỗi job `validate_*` fail sẽ dừng pipeline; kết quả ghi vào `meta.dq_results`.
- **Observability**: mỗi Spark job ghi 1 dòng (run_id, layer, status, rows_out,
  duration) vào `meta.job_log` → theo dõi rows/thời lượng/trạng thái từng tầng.
- **Spark**: jobs submit tới standalone cluster (`spark://spark-master:7077`); driver chạy
  trong container `airflow-scheduler` (client mode, jar parity giữa 2 image).
