#!/bin/bash
# Creates additional databases on first Postgres startup:
#   - metastore  (Hive Metastore backend, owned by hive user)
#   - superset   (Superset metadata)
#   - olist_source (simulated OLTP source - extension #3)
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Hive metastore user + db
    CREATE USER ${HIVE_DB_USER} WITH PASSWORD '${HIVE_DB_PASSWORD}';
    CREATE DATABASE ${HIVE_DB} OWNER ${HIVE_DB_USER};
    GRANT ALL PRIVILEGES ON DATABASE ${HIVE_DB} TO ${HIVE_DB_USER};

    -- Superset metadata db
    CREATE DATABASE ${SUPERSET_DB} OWNER ${POSTGRES_USER};

    -- Simulated OLTP source db (extension #3)
    CREATE DATABASE ${SOURCE_DB} OWNER ${POSTGRES_USER};
EOSQL

# create the olist_source schema inside the source db
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${SOURCE_DB}" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS olist_source;
EOSQL

echo "Additional databases created: ${HIVE_DB}, ${SUPERSET_DB}, ${SOURCE_DB}"
