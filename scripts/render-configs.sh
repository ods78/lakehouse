#!/usr/bin/env bash
# Render config templates that need runtime values (Ceph S3 credentials).
# Templates use {{PLACEHOLDER}} markers:
#   storage/clickhouse/config.d/storage.xml.tmpl
#   serving/trino/etc/catalog/lakehouse.properties.tmpl
set -euo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a

render() {
  local tmpl="$1" out="$2"
  sed -e "s|{{CEPH_ACCESS_KEY_ID}}|${CEPH_ACCESS_KEY_ID:-}|g" \
      -e "s|{{CEPH_SECRET_ACCESS_KEY}}|${CEPH_SECRET_ACCESS_KEY:-}|g" \
      -e "s|{{CEPH_RGW_HOST_PORT}}|${CEPH_RGW_HOST_PORT:-18800}|g" \
      -e "s|{{OPA_ADMIN_USERS}}|${OPA_ADMIN_USERS_SET:-}|g" \
      "$tmpl" > "$out"
  echo "rendered $out"
}

if [ ! -f .env ]; then
  echo "ERROR: .env missing — run ./lakehouse.sh init first" >&2
  exit 1
fi

render storage/clickhouse/config.d/storage.xml.tmpl storage/clickhouse/config.d/storage.xml
render serving/trino/etc/catalog/lakehouse.properties.tmpl serving/trino/etc/catalog/lakehouse.properties

# OPA admin set -> Rego set literal: {"admin","lakehouse-dbt",...}
OPA_ADMIN_USERS_SET=$(echo "${OPA_ADMIN_USERS:-admin}" | tr ',' '\n' | sed 's/^ *//;s/ *$//' | grep -v '^$' | sed 's/^/"/;s/$/"/' | paste -sd, -)
OPA_ADMIN_USERS_SET="{$OPA_ADMIN_USERS_SET}"
render ops/opa/policies/trino.rego.tmpl ops/opa/policies/trino.rego
