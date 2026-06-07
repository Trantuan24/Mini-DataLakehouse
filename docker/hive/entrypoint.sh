#!/bin/bash
set -e

echo "[hive-metastore] Initializing schema (ignored if already present)..."
/opt/hive/bin/schematool -dbType postgres -initSchema --verbose || \
  echo "[hive-metastore] Schema already initialized, continuing."

echo "[hive-metastore] Starting Hive Metastore service on :9083 ..."
exec /opt/hive/bin/hive --service metastore
