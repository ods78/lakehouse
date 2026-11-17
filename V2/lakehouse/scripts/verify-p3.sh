#!/bin/bash
set -e; cd "$(dirname "$0")/.."; set -a; . ./.env; set +a
ch(){ docker compose exec -T clickhouse clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" -q "$1"; }
ch "CREATE TABLE IF NOT EXISTS demo.cold_test (ts DateTime, v UInt32) ENGINE=MergeTree ORDER BY ts SETTINGS storage_policy='hot_cold'"
ch "INSERT INTO demo.cold_test SELECT now(), number FROM numbers(1000)"
ch "ALTER TABLE demo.cold_test MOVE PARTITION tuple() TO VOLUME 'cold'"
echo "parts by disk:"; ch "SELECT disk_name, sum(rows) FROM system.parts WHERE database='demo' AND table='cold_test' AND active GROUP BY disk_name"
echo "cold row count (read back from S3):"; ch "SELECT count() FROM demo.cold_test"
echo "iceberg catalog:"; curl -sf http://localhost:8181/v1/config | cut -c1-200; echo
