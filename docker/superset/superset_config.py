import os

# Metadata DB (Postgres)
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://airflow:airflow@postgres:5432/superset",
)

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change_me")

# Allow embedding / export and richer SQL Lab
FEATURE_FLAGS = {
    "DASHBOARD_RBAC": False,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": False,
}

SQLLAB_CTAS_NO_LIMIT = True
ROW_LIMIT = 50000
SUPERSET_WEBSERVER_TIMEOUT = 120

# Cache (simple in-memory; fine for a local thesis demo)
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG
ENABLE_PROXY_FIX = True
PUBLIC_ROLE_LIKE = "Gamma"