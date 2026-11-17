#!/usr/bin/env bash
# Apply the SQL Server CDC setup SQL (idempotent) + insert/update sample rows
# so Debezium emits snapshot + incremental events.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { echo "ERROR: .env missing" >&2; exit 1; }
# shellcheck disable=SC1091
set -a && . ./.env && set +a

SQL=/opt/mssql-tools18/bin/sqlcmd
[ -x "$(command -v docker)" ] || { echo "docker required" >&2; exit 1; }

echo "==> applying CDC setup SQL"
docker exec lakehouse-mssql $SQL -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -v DEBEZIUM_PASSWORD="$MSSQL_DEBEZIUM_PASSWORD" \
  -i /lakehouse-init/01-source-setup.sql

echo "==> producing an UPDATE to generate a live CDC event"
docker exec lakehouse-mssql $SQL -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" \
  -Q "USE lakehouse_src; UPDATE dbo.customers SET city = 'Berlin-East', updated_at = SYSDATETIME() WHERE name = 'Ava Müller';"

echo "SQL Server source ready (db: lakehouse_src, table: dbo.customers, CDC enabled)."
