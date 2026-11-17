#!/usr/bin/env bash
# Initialize Vault (one-time): init, capture unseal key + root token, unseal,
# enable KV v2 at secret/lakehouse and store a demo secret.
# Keys land in ops/vault/keys/init.json (DO NOT COMMIT).
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p ops/vault/keys

if [ -f ops/vault/keys/init.json ]; then
  echo "Vault already initialized (ops/vault/keys/init.json exists) — unsealing only."
else
  docker exec -e VAULT_ADDR=http://127.0.0.1:8200 lakehouse-vault \
    vault operator init -key-shares=1 -key-threshold=1 -format=json \
    > ops/vault/keys/init.json
  chmod 600 ops/vault/keys/init.json
  echo "unseal key + root token saved to ops/vault/keys/init.json"
fi

ROOT_TOKEN=$(python3 -c "import json;print(json.load(open('ops/vault/keys/init.json'))['root_token'])")
UNSEAL_KEY=$(python3 -c "import json;print(json.load(open('ops/vault/keys/init.json'))['unseal_keys_b64'][0])")

docker exec -e VAULT_ADDR=http://127.0.0.1:8200 lakehouse-vault vault operator unseal "$UNSEAL_KEY" >/dev/null

docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN="$ROOT_TOKEN" lakehouse-vault \
  sh -c '
    vault secrets enable -path=secret kv-v2 2>/dev/null || true
    vault kv put secret/lakehouse/postgres username="$POSTGRES_USER" password="$POSTGRES_PASSWORD" 2>/dev/null || \
    vault kv put secret/lakehouse/postgres username=unknown password=unknown
    vault kv get secret/lakehouse/postgres
  ' || true

echo "Vault ready: http://localhost:${VAULT_HOST_PORT:-18200} (root token in ops/vault/keys/init.json)"
