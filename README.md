# Mini Data Platform — Lakehouse (Olist Brazil)

Đồ án tốt nghiệp — Trần Duy Tuấn (2251061917). Mini Lakehouse end-to-end:
**Postgres → Bronze → Silver → Gold → Platinum → Superset**, điều phối bằng Airflow,
lưu trữ Iceberg trên MinIO, truy vấn bằng Trino.

> Thiết kế chi tiết: [`DESIGN.md`](./DESIGN.md) · Khung báo cáo: [`BaoCao_DATN_Outline.md`](./BaoCao_DATN_Outline.md)

## Kiến trúc

```
CSV ─▶ Postgres(olist_source) ─▶ Bronze ─▶ Silver ─▶ Gold ─▶ Platinum ─▶ Superset
                                  └────── Iceberg trên MinIO ──────┘        (qua Trino)
        Airflow điều phối · Great Expectations (DQ gate) · pytest (tests/)
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
Mở Airflow (http://localhost:8085) → bật/trigger DAG **`lakehouse_pipeline`**.
Thứ tự task:

```
load_source_to_postgres → ingest_raw_to_bronze → validate_bronze
→ transform_bronze_to_silver → validate_silver
→ build_gold_dims → build_gold_facts → validate_gold
→ build_platinum → run_etl_tests → notify_done
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
│   ├── common/                 # config, spark_session, data_quality
│   ├── bronze/                 # load_source, ingest, validate
│   ├── silver/                 # transform, validate
│   ├── gold/                   # build_dimensions, build_facts, validate
│   └── platinum/               # build_marts
├── dags/lakehouse_pipeline.py  # Airflow DAG
├── tests/                      # pytest (bronze/silver/gold)
├── scripts/                    # init buckets, upload raw
└── dataset/                    # 9 CSV Olist (đặt trực tiếp ở đây)
```

## Ghi chú thiết kế
- **Iceberg warehouse**: tất cả bảng nằm dưới bucket `warehouse` (`s3a://warehouse/<db>.db/`),
  Trino đọc qua catalog `iceberg` + Hive Metastore. Các bucket `bronze/silver/gold/platinum`
  vẫn được tạo cho dữ liệu raw/metadata và minh hoạ kiến trúc phân tầng.
- **Idempotent**: Bronze/Silver/Gold/Platinum dùng `createOrReplace` (overwrite) → chạy lại
  pipeline cho kết quả giống nhau.
- **DQ gate**: mỗi job `validate_*` fail sẽ dừng pipeline; kết quả ghi vào `meta.dq_results`.
- **Spark**: jobs submit tới standalone cluster (`spark://spark-master:7077`); driver chạy
  trong container `airflow-scheduler` (client mode, jar parity giữa 2 image).
