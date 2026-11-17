#!/usr/bin/env bash
# One-time Ceph bootstrap (idempotent):
#   1. extract client.bootstrap-rgw keyring into the ceph-bootstrap volume
#   2. prepare osd.0: keyring + Bluestore mkfs on a sparse block file
#      (Reef dropped Filestore; a regular-file block device is the standard
#       directory-backed single-node trick — only used space is allocated)
#   3. single-node tuning (config store): pool size 1, RGW frontend port,
#      warning thresholds
#   4. wait for HEALTH_OK
#   5. create the S3 user 'lakehouse' and write keys into .env
#   6. create all lake buckets via minio/mc, render configs, restart consumers
# Requires the storage layer up: ./lakehouse.sh up storage
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { echo "ERROR: .env missing — run ./lakehouse.sh init" >&2; exit 1; }
# shellcheck disable=SC1091
set -a && . ./.env && set +a

CEPH_IMAGE="${CEPH_IMAGE:-quay.io/ceph/daemon:latest-reef}"

echo "==> [1/6] extracting RGW bootstrap keyring from MON into ceph-bootstrap volume"
docker volume create lakehouse_ceph-bootstrap >/dev/null 2>&1 || true
docker exec lakehouse-ceph-mon sh -c '
  mkdir -p /var/lib/ceph/bootstrap-rgw
  if [ ! -s /var/lib/ceph/bootstrap-rgw/ceph.keyring ]; then
    ceph auth get client.bootstrap-rgw -o /var/lib/ceph/bootstrap-rgw/ceph.keyring \
      || ceph auth get-or-create client.bootstrap-rgw mon "allow profile bootstrap-rgw" \
           -o /var/lib/ceph/bootstrap-rgw/ceph.keyring
    chown 167:167 /var/lib/ceph/bootstrap-rgw/ceph.keyring
  fi'
docker exec lakehouse-ceph-mon sh -c 'cat /var/lib/ceph/bootstrap-rgw/ceph.keyring' \
  | docker run --rm -i -v lakehouse_ceph-bootstrap:/bk alpine \
    sh -c 'cat > /bk/ceph.keyring && chown 167:167 /bk/ceph.keyring'

echo "==> [2/6] preparing osd.0 (keyring + Bluestore mkfs on sparse block file)"
docker volume create lakehouse_ceph-osd-data >/dev/null 2>&1 || true
docker run --rm -v lakehouse_ceph-osd-data:/osd alpine sh -c '
  mkdir -p /osd/ceph-0
  if [ ! -f /osd/ceph-0-block ]; then truncate -s 60G /osd/ceph-0-block; fi
  chown -R 167:167 /osd'
docker exec lakehouse-ceph-mon sh -c '
  ceph auth get osd.0 2>/dev/null \
    || ceph auth get-or-create osd.0 osd "allow *" mon "allow profile osd" mgr "allow profile osd"' \
  > /tmp/osd0.keyring
docker cp /tmp/osd0.keyring lakehouse-ceph-mon:/tmp/osd0.keyring >/dev/null
docker run --rm -v lakehouse_ceph-osd-data:/osd -v /tmp:/hosttmp:ro alpine \
  sh -c 'cp /hosttmp/osd0.keyring /osd/ceph-0/keyring && chown -R 167:167 /osd/ceph-0'
rm -f /tmp/osd0.keyring && docker exec lakehouse-ceph-mon rm -f /tmp/osd0.keyring 2>/dev/null || true

# mkfs via ephemeral container (--entrypoint sh bypasses the Ceph entrypoint,
# which demands a CEPH_DAEMON scenario). Creates the block symlink in ceph-0.
docker run --rm --entrypoint /bin/sh --network lakehouse \
  -v lakehouse_ceph-config:/etc/ceph \
  -v lakehouse_ceph-osd-data:/var/lib/ceph/osd \
  "$CEPH_IMAGE" -c '
  if [ ! -f /var/lib/ceph/osd/ceph-0/mkfs_done ]; then
    rm -f /var/lib/ceph/osd/ceph-0/fsid
    ceph-osd -i 0 --mkfs --osd-objectstore bluestore \
      --osd-data /var/lib/ceph/osd/ceph-0 \
      --bluestore-block-path /var/lib/ceph/osd/ceph-0-block \
      --keyring /var/lib/ceph/osd/ceph-0/keyring && \
    chown -R 167:167 /var/lib/ceph/osd/ceph-0 && echo MKFS_OK
  else
    echo MKFS_SKIPPED_ALREADY
  fi'

echo "==> [3/6] single-node tuning (live config store + shared ceph.conf)"
docker exec lakehouse-ceph-mon sh -c '
  ceph config set global osd_pool_default_size 1
  ceph config set global osd_pool_default_min_size 1
  ceph config set mon mon_data_avail_warn 10
  ceph config set global mon_allow_pool_size_one true
  ceph config set mon mon_warn_on_pool_no_redundancy false
  ceph config set global rgw_frontends "beast port=8080"
  for p in $(ceph osd pool ls); do
    ceph osd pool set "$p" size 1 --yes-i-really-mean-it 2>/dev/null || true
    ceph osd pool set "$p" min_size 1 2>/dev/null || true
  done
  if ! grep -q "osd_pool_default_size" /etc/ceph/ceph.conf 2>/dev/null; then
    cat >> /etc/ceph/ceph.conf <<EOF

# single-node defaults (appended by bootstrap-ceph)
[global]
osd_pool_default_size = 1
osd_pool_default_min_size = 1
EOF
  fi'

echo "==> [4/6] restarting OSD/RGW to pick up prepared state, waiting HEALTH_OK"
docker restart lakehouse-ceph-osd lakehouse-ceph-rgw >/dev/null 2>&1 || true
# once the OSD has joined, the insecure global-id reclaim window is no longer
# needed — leaving it enabled keeps HEALTH_WARN forever and hangs this loop.
# mon_max_pg_per_osd is checked by BOTH mon and mgr (default 250; the rgw
# system pools alone push a single OSD over that)
docker exec lakehouse-ceph-mon sh -c '
  ceph config set mon auth_allow_insecure_global_id_reclaim false
  ceph config set mon mon_warn_on_insecure_global_id_reclaim_allowed false
  ceph config set mon mon_max_pg_per_osd 768
  ceph config set mgr mon_max_pg_per_osd 768' >/dev/null 2>&1 || true
for i in $(seq 1 90); do
  health=$(docker exec lakehouse-ceph-mon ceph health 2>/dev/null || echo "booting")
  echo "    ceph health: $health"
  case "$health" in
    HEALTH_OK) break ;;
    *) sleep 10 ;;
  esac
  [ "$i" = 90 ] && { echo "ERROR: Ceph did not reach HEALTH_OK. Check: docker logs lakehouse-ceph-osd lakehouse-ceph-rgw" >&2; exit 1; }
done

echo "==> [5/6] creating S3 user 'lakehouse'"
user_json=$(docker exec lakehouse-ceph-rgw radosgw-admin user create \
  --uid=lakehouse --display-name="LakeHouse S3" 2>/dev/null || true)

if [ -z "$user_json" ]; then
  # user already exists: fetch keys
  user_json=$(docker exec lakehouse-ceph-rgw radosgw-admin user info --uid=lakehouse)
fi

ACCESS_KEY=$(echo "$user_json" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['keys'][0]['access_key'])")
SECRET_KEY=$(echo "$user_json" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['keys'][0]['secret_key'])")

# upsert into .env (keep single occurrence)
upsert_env() {
  local key="$1" val="$2"
  if grep -q "^${key}=" .env 2>/dev/null; then
    sed -i "s|^${key}=.*|${key}=${val}|" .env
  else
    printf "%s=%s\n" "$key" "$val" >> .env
  fi
}
upsert_env CEPH_ACCESS_KEY_ID "$ACCESS_KEY"
upsert_env CEPH_SECRET_ACCESS_KEY "$SECRET_KEY"
echo "    keys written to .env (CEPH_ACCESS_KEY_ID / CEPH_SECRET_ACCESS_KEY)"

echo "==> [6/6] creating buckets"
aws_run() {
  docker run --rm --network lakehouse \
    -e AWS_ACCESS_KEY_ID="$ACCESS_KEY" -e AWS_SECRET_ACCESS_KEY="$SECRET_KEY" \
    -e AWS_DEFAULT_REGION="${S3_REGION:-us-east-1}" \
    amazon/aws-cli --endpoint-url http://ceph-rgw:8080 "$@"
}
for b in "$S3_BUCKET_ICEBERG" "$S3_BUCKET_LANDING" "$S3_BUCKET_CDC" "$S3_BUCKET_MLFLOW" "$S3_BUCKET_CLICKHOUSE" "$S3_BUCKET_BACKUPS"; do
  aws_run s3 mb "s3://$b" >/dev/null 2>&1 || true
done
aws_run s3 ls

echo ""
echo "==> re-rendering configs with S3 keys + restarting S3 consumers"
bash scripts/render-configs.sh >/dev/null
for c in lakehouse-clickhouse lakehouse-iceberg-rest lakehouse-trino lakehouse-mlflow; do
  docker ps --format '{{.Names}}' | grep -q "^$c$" && docker restart "$c" >/dev/null && echo "    restarted $c"
done || true

echo ""
echo "Ceph bootstrap complete."
