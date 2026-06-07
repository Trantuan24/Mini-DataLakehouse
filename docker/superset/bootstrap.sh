#!/bin/bash
set -e

echo "[superset] Upgrading metadata database..."
superset db upgrade

echo "[superset] Creating admin user (ignored if exists)..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN:-admin}" \
  --firstname Admin --lastname User \
  --email admin@example.com \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || true

echo "[superset] Initializing roles and permissions..."
superset init

echo "[superset] Registering Trino database connection..."
python /app/docker/create_trino_db.py || echo "[superset] Trino DB registration skipped/failed (can be added via UI)."

echo "[superset] Starting server on :8088 ..."
exec gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 4 \
  --timeout 120 \
  "superset.app:create_app()"
