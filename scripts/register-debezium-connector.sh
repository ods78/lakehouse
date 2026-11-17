#!/usr/bin/env bash
# Register the Debezium SQL Server CDC connector through the Connect REST API.
# Renders ingestion/debezium/mssql-cdc-customers.json with credentials from .env.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { echo "ERROR: .env missing" >&2; exit 1; }
# shellcheck disable=SC1091
set -a && . ./.env && set +a

# wait for Connect
for i in $(seq 1 30); do
  if curl -sf "http://localhost:${DEBEZIUM_HOST_PORT}/connectors" >/dev/null; then break; fi
  sleep 5
done

payload=$(sed "s|__MSSQL_DEBEZIUM_PASSWORD__|${MSSQL_DEBEZIUM_PASSWORD}|g" ingestion/debezium/mssql-cdc-customers.json)

name=$(echo "$payload" | python3 -c "import sys,json;print(json.load(sys.stdin)['name'])")

echo "==> registering connector: $name"
status=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Content-Type: application/json" \
  --data "$payload" \
  "http://localhost:${DEBEZIUM_HOST_PORT}/connectors")

if [ "$status" = "200" ] || [ "$status" = "201" ]; then
  echo "connector registered (HTTP $status)"
elif [ "$status" = "409" ]; then
  echo "connector already registered (HTTP 409) — OK (idempotent)"
  curl -s "http://localhost:${DEBEZIUM_HOST_PORT}/connectors/$name/status" | python3 -m json.tool || true
else
  echo "ERROR: connector registration failed (HTTP $status)" >&2
  exit 1
fi
