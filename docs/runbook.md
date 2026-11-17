# LakeHouse Runbook

## Daily operations
```bash
./lakehouse.sh status            # containers + volumes
./lakehouse.sh logs <service>    # e.g. logs airflow-scheduler
./lakehouse.sh smoke             # end-to-end checks
./lakehouse.sh urls              # endpoints + credentials
./lakehouse.sh backup            # db dumps + volumes + S3 upload
./lakehouse.sh restore 20260114-090000
./lakehouse.sh reset             # nuclear: down -v (asks confirmation)
```

## First-boot order (idempotent)
1. `./lakehouse.sh up storage && ./lakehouse.sh bootstrap-ceph`
2. `./lakehouse.sh up sources && ./lakehouse.sh seed-mssql`
3. `./lakehouse.sh up ingestion && ./lakehouse.sh register-cdc`
4. `./lakehouse.sh up processing`
5. `./lakehouse.sh up serving && ./lakehouse.sh starrocks-add-be`
6. `./lakehouse.sh up consumers`
7. `./lakehouse.sh up ops && ./lakehouse.sh vault-init`
8. `./lakehouse.sh smoke`

## Troubleshooting matrix
| Symptom | Check |
|---|---|
| Ceph OSD exits with "unrecognized scenario" | The reef image needs `command: ["osd_directory_single"]` + a pre-bootstrapped OSD dir — `bootstrap-ceph` handles it (Bluestore on a sparse `ceph-0-block` file, since Reef removed Filestore) |
| RGW loop: "bootstrap-rgw/ceph.keyring must exist" | `bootstrap-ceph` extracts `client.bootstrap-rgw` from the MON into the `ceph-bootstrap` volume, then restarts RGW |
| Ceph HEALTH_WARN "pool(s) have no replicas" | Expected for single-node size=1 pools — `bootstrap-ceph` silences it (`mon_warn_on_pool_no_redundancy=false`) |
| RGW ignores RGW_CIVETWEB_PORT (listens 7480) | Frontend is forced to `beast port=8080` via the config store in `bootstrap-ceph` |
| Iceberg 403 on S3 | keys empty in `.env` → re-run `bootstrap-ceph`, then `up` again (env reload) |
| Debezium connector failed | `curl localhost:18083/connectors/mssql-cdc-customers/status`; SQL Agent must be on (MSSQL_AGENT_ENABLED) |
| Spark job 401/NoSuchBucket | run `bootstrap-ceph`; verify bucket `lake-iceberg` via `aws-cli s3 ls` (endpoint http://localhost:18800) |
| Trino `catalog lakehouse does not exist` | config not rendered → `./lakehouse.sh init` (renders `catalog/lakehouse.properties`) |
| StarRocks FE/BE won't start (exit 0) | Image ships no default entrypoint — compose runs `/opt/starrocks/{fe,be}_entrypoint.sh starrocks-fe`; BE self-registers with the FE, so `starrocks-add-be` is only a manual fallback |
| Airflow DAG import error | `./lakehouse.sh logs airflow-scheduler`; DAG folder bind-mounted read-only |
| dbt fails auth | Trino runs unauthenticated by design (OPA gates access); check `profiles.yml` |
| OpenMetadata booting forever | needs several minutes; check `docker logs lakehouse-openmetadata-server` |
| NiFi login | https://localhost:18443/nifi, creds in `.env` (NIFI_SINGLE_USER_*) |

## SSO wiring (Keycloak, optional hardening)
Realm `lakehouse` is auto-imported with clients. To move a UI from basic auth to
Keycloak OIDC, use the pre-registered client (public) and redirect URI from
`ops/keycloak/import/lakehouse-realm.json`. Examples:
- **Grafana**: set `GF_AUTH_GENERIC_OAUTH_*` env (client `grafana`,
  `http://localhost:13000/login/generic_oauth`), restart.
- **JupyterHub**: switch `authenticator_class` to `generic-oauth` with the
  `jupyterhub` client.
- **Superset**: add OIDC via `superset_config.py` (Flask-OIDC) using client `superset`.
Keycloak admin: http://localhost:18088 (admin / $KEYCLOAK_ADMIN_PASSWORD).

## Secrets management (Vault)
`vault-init` stores Postgres creds at `secret/lakehouse/postgres` (KV v2).
```bash
export VAULT_ADDR=http://localhost:18200
export VAULT_TOKEN=$(python3 -c "import json;print(json.load(open('ops/vault/keys/init.json'))['root_token'])")
vault kv get secret/lakehouse/postgres
```

## Governance (Ranger/OPA)
OPA is the active policy decision point (Trino). To extend: edit
`ops/opa/policies/trino.rego` and restart `lakehouse-opa`. For full Apache Ranger,
add a Ranger admin container with Solr + its MySQL schema, then point Trino's
`ranger` access control at it — OPA stays for fast-path policies.

## StarRocks acceleration (materialized views)
```sql
-- from any mysql client at localhost:19030
SET CATALOG iceberg_lakehouse;         -- create with: CREATE EXTERNAL CATALOG
CREATE MATERIALIZED VIEW mv_customers_by_city
  DISTRIBUTED BY HASH(city)
  AS SELECT city, count(*) cnt FROM dim_customers GROUP BY city;
```
(FE also ingests the Trino/StarRocks catalog config from the runbook addendum
below.)

## Iceberg catalog in StarRocks (one-time)
```sql
CREATE EXTERNAL CATALOG iceberg_lakehouse
PROPERTIES (
  "type"="iceberg",
  "iceberg.catalog.type"="rest",
  "uri"="http://iceberg-rest:8181",
  "warehouse"="s3://lake-iceberg/warehouse",
  "aws.s3.access_key"="<CEPH_ACCESS_KEY_ID>",
  "aws.s3.secret_key"="<CEPH_SECRET_ACCESS_KEY>",
  "aws.s3.endpoint"="http://ceph-rgw:8080",
  "aws.s3.enable_path_style_access"="true"
);
```

## NiFi → S3 landing (target flow)
Use the bundled processor graph concept: ListS3/GetFile → PublishKafkaRecord or
PutS3Object with `Object Key Prefix = landing/<source>/$(now:%Y/%m/%d)/`,
S3 endpoint `http://ceph-rgw:8080`, creds from Vault/`.env`. Files appear in the
`lakehouse_batch` DAG's landing check.

## DR: full rebuild from backups
1. Fresh host + repo checkout + `.env` (restore from git-ignored copy or regenerate).
2. `./lakehouse.sh init && ./lakehouse.sh up all`
3. `./lakehouse.sh restore <ts>` (restores Postgres metadata + volumes)
4. `./lakehouse.sh bootstrap-ceph` (S3 user recreate) → re-run `up all`
5. Verify with `./lakehouse.sh smoke`.
