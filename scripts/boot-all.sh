#!/usr/bin/env bash
# First-boot orchestration for the full LakeHouse stack.
# Runs every phase in dependency order with the one-time bootstrap steps.
# Safe to re-run: all steps are idempotent.
set -uo pipefail
cd "$(dirname "$0")/.."

step() { echo ""; echo "============================================================"; echo "==> $1"; echo "============================================================"; }

step "storage (Postgres + Ceph + ClickHouse)"
./lakehouse.sh up storage || exit 1

step "bootstrap-ceph (S3 user + buckets)"
./lakehouse.sh bootstrap-ceph || echo "WARN: bootstrap-ceph reported a problem — continuing"

step "sources (SQL Server)"
./lakehouse.sh up sources || exit 1

step "seed-mssql (CDC enable + sample data)"
./lakehouse.sh seed-mssql || echo "WARN: seed-mssql failed — continuing"

step "ingestion (Kafka + Schema Registry + Debezium + NiFi)"
./lakehouse.sh up ingestion || exit 1

step "register-cdc (Debezium connector)"
./lakehouse.sh register-cdc || echo "WARN: register-cdc failed — continuing"

step "catalog (Iceberg REST)"
./lakehouse.sh up catalog || exit 1

step "processing (Spark + Airflow)"
./lakehouse.sh up processing || exit 1

step "serving (Trino + StarRocks)"
./lakehouse.sh up serving || exit 1

step "starrocks-add-be"
./lakehouse.sh starrocks-add-be || echo "WARN: BE registration failed — continuing"

step "consumers (Superset + JupyterHub + MLflow)"
./lakehouse.sh up consumers || exit 1

step "ops (Vault + Keycloak + OpenMetadata + observability)"
./lakehouse.sh up ops || exit 1

step "vault-init"
./lakehouse.sh vault-init || echo "WARN: vault-init failed — continuing"

step "status"
./lakehouse.sh status

echo ""
echo "BOOT SEQUENCE FINISHED — run ./lakehouse.sh smoke to verify integration"
