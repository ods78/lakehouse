#!/usr/bin/env bash
# End-to-end smoke test across the integrated stack.
# Usage: ./lakehouse.sh smoke
set -uo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a

pass=0; fail=0
check() {
  local name="$1"; shift
  echo -n "[$1] ${name}: "
  if [ "$1" = "pass" ]; then echo "PASS"; pass=$((pass+1)); else echo "FAIL — $2"; fail=$((fail+1)); fi
}
run_pass() { check "$1" pass; }
run_fail() { check "$1" fail "$2"; }

echo "================ LakeHouse smoke test ================"

# 1. Postgres
docker exec lakehouse-postgres psql -U "$POSTGRES_USER" -d postgres -tAc "SELECT count(*) FROM pg_database WHERE datname='iceberg_catalog';" \
  | grep -q 1 && run_pass "postgres" || run_fail "postgres" "database iceberg_catalog missing"

# 2. Kafka
docker exec lakehouse-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
  | grep -q "lakehouse.lakehouse_src.dbo.customers" && run_pass "kafka (CDC topic)" \
  || run_fail "kafka (CDC topic)" "run ./lakehouse.sh register-cdc && ./lakehouse.sh seed-mssql"

# 3. Schema registry
curl -sf "http://localhost:${SCHEMA_REGISTRY_HOST_PORT}/subjects" >/dev/null && run_pass "schema-registry" || run_fail "schema-registry" "not reachable"

# 4. Iceberg REST catalog
curl -sf "http://localhost:${ICEBERG_REST_HOST_PORT}/v1/config" >/dev/null && run_pass "iceberg-rest" || run_fail "iceberg-rest" "not reachable"

# 5. Spark Iceberg round-trip through catalog + S3
if docker exec lakehouse-spark-master sh -c \
  "CEPH_ACCESS_KEY_ID='${CEPH_ACCESS_KEY_ID:-}' CEPH_SECRET_ACCESS_KEY='${CEPH_SECRET_ACCESS_KEY:-}' \
   /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/write_read_smoke.py" \
  2>/dev/null | grep -q "SMOKE OK"; then
  run_pass "spark-iceberg-s3 roundtrip"
else
  run_fail "spark-iceberg-s3 roundtrip" "see spark logs"
fi

# 6. Trino query over the same Iceberg table (OPA-protected — must auth as admin)
docker run --rm --network lakehouse trinodb/trino:${TRINO_VERSION} \
  trino --server http://trino:8080 --user admin --catalog lakehouse --execute "SELECT count(*) FROM lakehouse.bronze.smoke_test" 2>/dev/null \
  | grep -qE '^"?2"?$' && run_pass "trino reads iceberg" || run_fail "trino reads iceberg" "table missing?"

# 7. ClickHouse hot tier + TTL policy
docker exec lakehouse-clickhouse clickhouse-client --user "${CLICKHOUSE_USER}" --password "${CLICKHOUSE_PASSWORD}" \
  -q "SELECT count() > 0 FROM lakehouse.hot_events" 2>/dev/null | grep -q 1 \
  && run_pass "clickhouse hot tier" || run_fail "clickhouse hot tier" "table empty/missing"

# 8. Airflow
curl -sf "http://localhost:${AIRFLOW_HOST_PORT}/health" | grep -q healthy && run_pass "airflow" || run_fail "airflow" "not healthy"

# 9. Superset
curl -sf "http://localhost:${SUPERSET_HOST_PORT}/health" | grep -q OK && run_pass "superset" || run_fail "superset" "not reachable"

# 10. MLflow
curl -sf "http://localhost:${MLFLOW_HOST_PORT}/health" >/dev/null && run_pass "mlflow" || run_fail "mlflow" "not reachable"

# 11. JupyterHub
curl -sf "http://localhost:${JUPYTERHUB_HOST_PORT}/hub/api/" >/dev/null && run_pass "jupyterhub" || run_fail "jupyterhub" "not reachable"

# 12. Keycloak
curl -sf "http://localhost:${KEYCLOAK_HOST_PORT}/realms/lakehouse" >/dev/null && run_pass "keycloak realm" || run_fail "keycloak realm" "realm not imported"

# 13. OpenMetadata
curl -sf "http://localhost:${OPENMETADATA_HOST_PORT}/api/v1/system/config/jwks" >/dev/null && run_pass "openmetadata" || run_fail "openmetadata" "not ready yet (boot takes minutes)"

# 14. OPA decision (response shape: {"result":true|false})
curl -sf -X POST "http://localhost:${OPA_HOST_PORT}/v1/data/trino/allow" \
  -H "Content-Type: application/json" \
  -d '{"input":{"context":{"identity":{"user":"admin"}}}}' | grep -q '"result":true' \
  && run_pass "opa policy" || run_fail "opa policy" "policy not allowing admin"

# 15. HAProxy edge
curl -s -o /dev/null -w "%{http_code}" -H "Host: superset.lakehouse.local" \
  "http://localhost:${HAPROXY_HTTP_HOST_PORT}/health" | grep -q 200 && run_pass "haproxy edge" || run_fail "haproxy edge" "routing broken"

# 16. Loki receives logs
curl -sf "http://localhost:${LOKI_HOST_PORT}/loki/api/v1/labels" >/dev/null && run_pass "loki" || run_fail "loki" "not reachable"

echo "======================================================"
echo "RESULT: ${pass} passed, ${fail} failed"
[ "$fail" -eq 0 ] || exit 1
