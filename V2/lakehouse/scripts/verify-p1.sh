#!/bin/bash
set -e; cd "$(dirname "$0")/.."; set -a; . ./.env; set +a
q(){ docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -d shop -h -1 -Q "SET NOCOUNT ON; $1"; }
echo "orders rows:"; q "SELECT COUNT(*) FROM dbo.orders"
echo "cdc enabled (1 = yes):"; q "SELECT is_cdc_enabled FROM sys.databases WHERE name='shop'"
