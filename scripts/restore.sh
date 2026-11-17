#!/usr/bin/env bash
# Restore LakeHouse from ./backups/<ts>/ (produced by scripts/backup.sh).
# Usage: ./lakehouse.sh restore <ts>
set -euo pipefail
cd "$(dirname "$0")/.."

TS="${1:?usage: lakehouse.sh restore <timestamp, e.g. 20260114-090000>}"
SRC="backups/$TS"
[ -d "$SRC" ] || { echo "ERROR: $SRC not found" >&2; exit 1; }

# shellcheck disable=SC1091
[ -f .env ] && set -a && . ./.env && set +a

echo "==> restoring PostgreSQL databases"
for dump in "$SRC"/pg-*.dump; do
  db=$(basename "$dump" .dump); db=${db#pg-}
  docker exec -i lakehouse-postgres psql -U "$POSTGRES_USER" -d postgres \
    -c "DROP DATABASE IF EXISTS $db WITH (FORCE);" \
    -c "CREATE DATABASE $db;"
  docker exec -i lakehouse-postgres pg_restore -U "$POSTGRES_USER" -d "$db" --no-owner < "$dump" || true
done

echo "==> restoring named volumes"
for arch in "$SRC"/vol-*.tar.gz; do
  [ -e "$arch" ] || continue
  base=$(basename "$arch" .tar.gz); vol="lakehouse_${base#vol-}"
  docker volume create "$vol" >/dev/null
  docker run --rm -v "$vol":/dst -v "$PWD/$arch":/backup.tar.gz:ro alpine \
    sh -c "cd /dst && tar xzf /backup.tar.gz"
  echo "    $vol restored"
done

echo "Restore complete. Restart the stack: ./lakehouse.sh up"
