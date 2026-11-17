#!/usr/bin/env bash
# Backup LakeHouse: PostgreSQL dumps + named-volume archives into ./backups/<ts>/
# Usage: ./lakehouse.sh backup
set -euo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a

TS=$(date +%Y%m%d-%H%M%S)
OUT="backups/$TS"
mkdir -p "$OUT"

echo "==> dumping PostgreSQL metadata databases"
for db in iceberg_catalog airflow keycloak superset nifi mlflow openmetadata_db; do
  docker exec lakehouse-postgres pg_dump -U "$POSTGRES_USER" -Fc "$db" > "$OUT/pg-$db.dump"
done

echo "==> archiving named volumes (data/state)"
VOLUMES=(
  lakehouse_kafka-data
  lakehouse_nifi-flowfile
  lakehouse_nifi-database
  lakehouse_nifi-state
  lakehouse_clickhouse-data
  lakehouse_ceph-config
  lakehouse_ceph-mon-data
  lakehouse_vault-data
  lakehouse_jupyterhub-data
  lakehouse_grafana-data
  lakehouse_prometheus-data
)
for v in "${VOLUMES[@]}"; do
  if docker volume inspect "$v" >/dev/null 2>&1; then
    docker run --rm -v "$v":/src:ro -v "$PWD/$OUT":/backup alpine \
      tar czf "/backup/vol-$(echo "$v" | sed 's|lakehouse_||').tar.gz" -C /src .
    echo "    $v archived"
  fi
done

echo "==> uploading to S3 lake-backups bucket (if configured)"
if [ -n "${CEPH_ACCESS_KEY_ID:-}" ]; then
  docker run --rm --network lakehouse \
    -v "$PWD/$OUT":/data:ro \
    -e AWS_ACCESS_KEY_ID="$CEPH_ACCESS_KEY_ID" \
    -e AWS_SECRET_ACCESS_KEY="$CEPH_SECRET_ACCESS_KEY" \
    -e AWS_DEFAULT_REGION="${S3_REGION}" \
    amazon/aws-cli --endpoint-url http://ceph-rgw:8080 s3 cp /data "s3://$S3_BUCKET_BACKUPS/$TS/" --recursive >/dev/null && \
    echo "    uploaded to s3://$S3_BUCKET_BACKUPS/$TS/"
fi

echo "Backup complete: $OUT"
