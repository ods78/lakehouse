#!/usr/bin/env bash
# Register the StarRocks BE with the FE and wait until it is alive.
set -euo pipefail
cd "$(dirname "$0")/.."

MY_SQL="mysql -h starrocks-fe -P 9030 -uroot --connect-timeout=10"

echo "==> adding backend starrocks-be:9050 (idempotent)"
docker run --rm --network lakehouse mysql:8.0 sh -c "
  $MY_SQL -e \"ALTER SYSTEM ADD BACKEND 'starrocks-be:9050'\" 2>/dev/null || true;
  for i in \$(seq 1 24); do
    echo \"waiting for BE (\$i/24)...\";
    out=\$($MY_SQL -e 'SHOW BACKENDS' 2>/dev/null || echo '');
    echo \"\$out\";
    if echo \"\$out\" | awk -F'\\\\\\\\t' 'NR>1 && \$13 == \"1\" {found=1} END{exit found?0:1}'; then
      echo 'BACKEND ALIVE'; exit 0;
    fi;
    sleep 5;
  done;
  echo 'ERROR: backend did not become alive' >&2; exit 1"
