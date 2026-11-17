#!/usr/bin/env bash
# =============================================================================
# LakeHouse control script
#   ./lakehouse.sh init                  create .env (if missing), dirs, render configs
#   ./lakehouse.sh up [PHASE|all]        start a phase (dependencies included)
#   ./lakehouse.sh down [PHASE|all]      stop
#   ./lakehouse.sh status | logs | restart | pull | build
#   ./lakehouse.sh bootstrap-ceph        S3 user + buckets (after first `up storage`)
#   ./lakehouse.sh seed-mssql            enable CDC + sample data
#   ./lakehouse.sh register-cdc          Debezium connector registration
#   ./lakehouse.sh starrocks-add-be      register BE with StarRocks FE
#   ./lakehouse.sh vault-init            init/unseal Vault
#   ./lakehouse.sh smoke                 end-to-end integration test
#   ./lakehouse.sh backup | restore TS | reset | hosts | urls | help
#
# Phases: base sources ingestion storage catalog processing serving consumers ops ops-ha
# `all` starts every layer of the architecture in dependency order.
# =============================================================================
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

PHASES=(base sources ingestion storage catalog processing serving consumers ops ops-ha)

compose_files_for() {
  local phase="$1"
  local files=()
  case "$phase" in
    base)       files=(docker-compose.yml) ;;
    sources)    files=(docker-compose.yml docker-compose.sources.yml) ;;
    ingestion)  files=(docker-compose.yml docker-compose.sources.yml docker-compose.storage.yml docker-compose.ingestion.yml) ;;
    storage)    files=(docker-compose.yml docker-compose.storage.yml) ;;
    catalog)    files=(docker-compose.yml docker-compose.storage.yml docker-compose.catalog.yml) ;;
    processing) files=(docker-compose.yml docker-compose.sources.yml docker-compose.storage.yml docker-compose.catalog.yml docker-compose.processing.yml) ;;
    serving)    files=(docker-compose.yml docker-compose.storage.yml docker-compose.catalog.yml docker-compose.serving.yml) ;;
    consumers)  files=(docker-compose.yml docker-compose.storage.yml docker-compose.catalog.yml docker-compose.serving.yml docker-compose.consumers.yml) ;;
    ops)        files=(docker-compose.yml docker-compose.ops.yml) ;;
    ops-ha)     files=(docker-compose.yml docker-compose.ops.yml docker-compose.keepalived.yml) ;;
    all)        files=(docker-compose.yml docker-compose.sources.yml docker-compose.storage.yml docker-compose.ingestion.yml docker-compose.catalog.yml docker-compose.processing.yml docker-compose.serving.yml docker-compose.consumers.yml docker-compose.ops.yml) ;;
    *) echo "Unknown phase: $phase (one of: ${PHASES[*]} all)" >&2; exit 1 ;;
  esac
  local args=()
  for f in "${files[@]}"; do args+=(-f "$f"); done
  printf '%s\n' "${args[@]}"
}

gen_secret() { openssl rand -hex 12; }

cmd_init() {
  if [ ! -f .env ]; then
    echo "==> generating .env with fresh secrets"
    cp .env.example .env
    # inherit host proxy settings for build-time access
    [ -n "${HTTP_PROXY:-}" ] && sed -i "s|^HTTP_PROXY=$|HTTP_PROXY=${HTTP_PROXY}|" .env
    [ -n "${HTTPS_PROXY:-}" ] && sed -i "s|^HTTPS_PROXY=$|HTTPS_PROXY=${HTTPS_PROXY}|" .env
    # MSSQL password needs upper+lower+digit complexity
    sed -i "s|POSTGRES_PASSWORD=CHANGE_ME|POSTGRES_PASSWORD=$(gen_secret)|" .env
    sed -i "s|CLICKHOUSE_PASSWORD=CHANGE_ME|CLICKHOUSE_PASSWORD=$(gen_secret)|" .env
    sed -i "s|AIRFLOW_FERNET_KEY=CHANGE_ME_GENERATED_BY_INIT|AIRFLOW_FERNET_KEY=$(openssl rand -base64 32 \| tr -d '\n')|" .env
    sed -i "s|AIRFLOW_ADMIN_PASSWORD=CHANGE_ME|AIRFLOW_ADMIN_PASSWORD=$(gen_secret)|" .env
    sed -i "s|AIRFLOW_WEBSERVER_SECRET_KEY=CHANGE_ME|AIRFLOW_WEBSERVER_SECRET_KEY=$(gen_secret)|" .env
    sed -i "s|SUPERSET_SECRET_KEY=CHANGE_ME|SUPERSET_SECRET_KEY=$(gen_secret)|" .env
    sed -i "s|SUPERSET_ADMIN_PASSWORD=CHANGE_ME|SUPERSET_ADMIN_PASSWORD=$(gen_secret)|" .env
    sed -i "s|NIFI_SINGLE_USER_PASSWORD=CHANGE_ME_MIN_12_CHARS|NIFI_SINGLE_USER_PASSWORD=$(gen_secret)|" .env
    sed -i "s|NIFI_SENSITIVE_PROPS_KEY=CHANGE_ME|NIFI_SENSITIVE_PROPS_KEY=$(gen_secret)|" .env
    sed -i "s|MSSQL_SA_PASSWORD=CHANGE_ME_Aa1|MSSQL_SA_PASSWORD=Lk$(gen_secret)Aa1|" .env
    sed -i "s|MSSQL_DEBEZIUM_PASSWORD=CHANGE_ME|MSSQL_DEBEZIUM_PASSWORD=$(gen_secret)|" .env
    sed -i "s|KEYCLOAK_ADMIN_PASSWORD=CHANGE_ME|KEYCLOAK_ADMIN_PASSWORD=$(gen_secret)|" .env
    sed -i "s|GRAFANA_ADMIN_PASSWORD=CHANGE_ME|GRAFANA_ADMIN_PASSWORD=$(gen_secret)|" .env
  else
    echo "==> .env exists — keeping it"
  fi

  echo "==> rendering runtime configs"
  bash scripts/render-configs.sh

  echo "==> building custom images (spark, airflow, jupyterhub)"
  bash "$0" build >/dev/null 2>&1 || true

  echo ""
  echo "init done. Next:"
  echo "  ./lakehouse.sh up storage      # Ceph + ClickHouse"
  echo "  ./lakehouse.sh bootstrap-ceph  # S3 user + buckets"
  echo "  ./lakehouse.sh up all          # full stack"
}

cmd_up() {
  local phase="${1:-all}"
  [ "$phase" = "all" ] || validate_phase "$phase"
  echo "==> docker compose up -d [$phase]"
  docker compose $(compose_files_for "$phase") up -d --build
  if [ "$phase" = "all" ]; then
    echo ""
    echo "Post-up steps:"
    echo "  ./lakehouse.sh bootstrap-ceph    # once, after first storage boot"
    echo "  ./lakehouse.sh seed-mssql        # once"
    echo "  ./lakehouse.sh register-cdc      # once (after ingestion is healthy)"
    echo "  ./lakehouse.sh starrocks-add-be  # once"
    echo "  ./lakehouse.sh vault-init        # once"
    echo "  ./lakehouse.sh smoke             # verify integration"
  fi
}

cmd_down() {
  local phase="${1:-all}"
  echo "==> docker compose down [$phase]"
  if [ "$phase" = "all" ]; then
    docker compose $(compose_files_for all) down
  else
    docker compose $(compose_files_for "$phase") down
  fi
}

cmd_status() {
  echo "==> lakehouse containers"
  docker ps --filter "label=com.docker.compose.project=lakehouse" \
    --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' | sort
  echo ""
  echo "==> volumes"
  docker volume ls --filter name=lakehouse_ --format '{{.Name}}' | sort | tr '\n' ' '; echo ""
}

cmd_logs() {
  if [ $# -ge 1 ]; then
    docker logs -f --tail=200 "lakehouse-$1" 2>/dev/null || docker logs -f --tail=200 "$1"
  else
    docker compose $(compose_files_for all) logs -f --tail=50
  fi
}

cmd_build() {
  docker compose $(compose_files_for processing) build spark-master airflow-webserver
  docker compose $(compose_files_for consumers) build jupyterhub
}

cmd_smoke()  { bash scripts/smoke-test.sh; }
cmd_ceph()   { bash scripts/bootstrap-ceph.sh; }
cmd_mssql()  { bash scripts/seed-mssql.sh; }
cmd_cdc()    { bash scripts/register-debezium-connector.sh; }
cmd_sr_be()  { bash scripts/starrocks-add-backend.sh; }
cmd_vault()  { bash scripts/vault-init.sh; }
cmd_backup() { bash scripts/backup.sh; }
cmd_restore(){ bash scripts/restore.sh "${1:?usage: lakehouse.sh restore <timestamp>}"; }

cmd_reset() {
  echo "WARNING: this stops the stack and DELETES all lakehouse volumes (data loss)."
  read -r -p "Type 'lakehouse' to confirm: " c
  [ "$c" = "lakehouse" ] || { echo "aborted"; exit 1; }
  docker compose $(compose_files_for all) down -v --remove-orphans
  echo "reset done (configs and ./backups preserved)"
}

cmd_hosts() {
  cat <<EOF | sudo tee -a /etc/hosts

# --- lakehouse stack ---
127.0.0.1 superset.lakehouse.local airflow.lakehouse.local nifi.lakehouse.local jupyter.lakehouse.local mlflow.lakehouse.local keycloak.lakehouse.local metadata.lakehouse.local grafana.lakehouse.local trino.lakehouse.local clickhouse.lakehouse.local vault.lakehouse.local starrocks.lakehouse.local kafka-ui.lakehouse.local rgw.lakehouse.local
EOF
  echo "hosts updated"
}

cmd_urls() {
  # shellcheck disable=SC1091
  set -a; [ -f .env ] && . ./.env; set +a
  cat <<EOF

UI endpoints (direct host ports):
  Superset        http://localhost:${SUPERSET_HOST_PORT}       (admin / \$SUPERSET_ADMIN_PASSWORD)
  Airflow         http://localhost:${AIRFLOW_HOST_PORT}       (admin / \$AIRFLOW_ADMIN_PASSWORD)
  NiFi            https://localhost:${NIFI_HOST_PORT}/nifi    (admin / \$NIFI_SINGLE_USER_PASSWORD)
  Kafka UI        http://localhost:${KAFKA_UI_HOST_PORT}
  Trino           http://localhost:${TRINO_HOST_PORT}
  StarRocks FE    http://localhost:${STARROCKS_HTTP_HOST_PORT}   (MySQL: localhost:${STARROCKS_MYSQL_HOST_PORT})
  ClickHouse      http://localhost:${CLICKHOUSE_HTTP_HOST_PORT}/play
  JupyterHub      http://localhost:${JUPYTERHUB_HOST_PORT}       (user: admin)
  MLflow          http://localhost:${MLFLOW_HOST_PORT}
  Keycloak        http://localhost:${KEYCLOAK_HOST_PORT}       (admin / \$KEYCLOAK_ADMIN_PASSWORD)
  OpenMetadata    http://localhost:${OPENMETADATA_HOST_PORT}   (admin / admin, first login)
  Vault           http://localhost:${VAULT_HOST_PORT}         (root token in ops/vault/keys/init.json)
  Grafana         http://localhost:${GRAFANA_HOST_PORT}       (admin / \$GRAFANA_ADMIN_PASSWORD)
  Prometheus      http://localhost:${PROMETHEUS_HOST_PORT}
  OPA             http://localhost:${OPA_HOST_PORT}

Via HAProxy edge (after ./lakehouse.sh hosts):
  http://superset.lakehouse.local:${HAPROXY_HTTP_HOST_PORT}   etc.
  HAProxy stats: http://localhost:${HAPROXY_STATS_HOST_PORT}/stats
EOF
}

validate_phase() {
  local ok=0
  for p in "${PHASES[@]}"; do [ "$p" = "$1" ] && ok=1; done
  [ "$ok" = 1 ] || { echo "Unknown phase: $1 (one of: ${PHASES[*]} all)" >&2; exit 1; }
}

cmd="${1:-help}"
case "$cmd" in
  init)       shift; cmd_init "$@" ;;
  up)         shift; cmd_up "${1:-all}" ;;
  down)       shift; cmd_down "${1:-all}" ;;
  stop)       shift; docker compose $(compose_files_for "${1:-all}") stop ;;
  restart)    shift; cmd_down "${1:-all}"; cmd_up "${1:-all}" ;;
  status|ps)  cmd_status ;;
  logs)       shift; cmd_logs "$@" ;;
  build)      cmd_build ;;
  pull)       docker compose $(compose_files_for all) pull ;;
  smoke)      cmd_smoke ;;
  bootstrap-ceph) cmd_ceph ;;
  seed-mssql) cmd_mssql ;;
  register-cdc) cmd_cdc ;;
  starrocks-add-be) cmd_sr_be ;;
  vault-init) cmd_vault ;;
  backup)     cmd_backup ;;
  restore)    shift; cmd_restore "$@" ;;
  reset)      cmd_reset ;;
  hosts)      cmd_hosts ;;
  urls)       cmd_urls ;;
  help|--help|-h) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//' ;;
  *) echo "unknown command: $cmd (see ./lakehouse.sh help)" >&2; exit 1 ;;
esac
