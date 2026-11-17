"""Superset configuration — wired to the LakeHouse shared PostgreSQL."""
import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://{user}:{password}@lakehouse-postgres:5432/superset".format(
        user=os.environ["POSTGRES_USER"], password=os.environ["POSTGRES_PASSWORD"]
    )
)

SQLLAB_TIMEOUT = 120
SUPERSET_WEBSERVER_TIMEOUT = 120
SUPERSET_WEBSERVER_PORT = 8088
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 3600}

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
}

# Recommended presets to create once Superset is up (Connection > Database):
#   Trino     : trino://lakehouse@trino:8080/lakehouse       (SQLAlchemy URI: trino://trino:8080/lakehouse)
#   StarRocks : mysql://root@starrocks-fe:9030/lakehouse    (dialect: MySQL)
#   ClickHouse: clickhousedb+connect://dwh:***@clickhouse:9000/lakehouse

# ---------------------------------------------------------------------------
# Keycloak SSO (Phase 8) — uncomment and set the redirect URL in the Keycloak
# client 'superset' after registering the app:
# from flask_appbuilder.security.manager import FAB_OIDC_AUTH?  # see docs/runbook.md
# ---------------------------------------------------------------------------
