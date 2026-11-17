# LakeHouse — Architecture & Integration Contract

## Networks
One user-defined bridge `lakehouse` (172.31.100.0/24). Every service is addressable
by its compose service name. Static IPs reserved for Ceph (MON .2, MGR .3, OSD .4,
RGW .5). Ceph and StarRocks use `priority_networks = 172.31.100.0/24`.

## Storage conventions (Ceph RGW, S3 API @ http://ceph-rgw:8080)
| Bucket | Owner | Contents |
|---|---|---|
| `lake-iceberg` | Iceberg REST catalog | `warehouse/<ns>/<table>/` ACID data+metadata |
| `lake-landing` | NiFi | raw batch/file/API payloads (by source/date) |
| `lake-cdc` | Debezium consumers | archived CDC payloads (optional) |
| `lake-mlflow` | MLflow | run artifacts |
| `lake-clickhouse` | ClickHouse | TTL-offloaded cold parts of `lakehouse.hot_events` |
| `lake-backups` | backup script | `backups/<ts>/` archives |

S3 credentials live in `.env` (`CEPH_ACCESS_KEY_ID/CEPH_SECRET_ACCESS_KEY`), created
by `./lakehouse.sh bootstrap-ceph`.

## The single Iceberg catalog
`iceberg-rest` (tabulario 1.6.0) = **the** catalog:
- backend: `lakehouse-postgres:5432/iceberg_catalog` (JdbcCatalog)
- storage: S3FileIO against Ceph RGW (signature-based, clients need no Hadoop S3A)
- namespace conventions: `bronze` (raw CDC), `silver` (current state), `analytics*` (dbt)

Clients wired to it:
| Client | Config |
|---|---|
| Spark 3.5 | `spark.sql.catalog.lakehouse.type=rest`, `uri=http://iceberg-rest:8181` (jobs set it programmatically) |
| Trino 476 | `catalog/lakehouse.properties` — `iceberg.catalog.type=rest`, `iceberg.rest-catalog.uri`, native S3 FS with path-style access |
| StarRocks | add Iceberg external catalog via FE SQL (see runbook) |

Because the REST catalog hands clients its S3 config, all clients see the **same**
tables/metadata: write in Spark, read in Trino/StarRocks instantly, with ACID
snapshots and time travel.

## Data pipeline integration
1. **Contracts**: Debezium writes Debezium-JSON envelopes; Schema Registry holds
   schemas for any Avro producers (NiFi/ML). `lakehouse` broker auto-creates topics
   `lakehouse.lakehouse_src.dbo.customers`, `connect-*`, `schemahistory.*`.
2. **Bronze** (`spark/jobs/bronze_cdc.py`): Kafka → Debezium JSON parse → append-only
   `lakehouse.bronze.customers` with `kafka_offset` watermark (idempotent re-runs).
3. **Silver** (`spark/jobs/silver_customers.py`): latest-event-per-id dedup →
   `lakehouse.silver.customers` (overwrite partitions).
4. **DQ gate** (`great-expectations/dq_checks.py`): reads silver via Trino, validates
   suite (non-null id/name/email, email regex) — GX 1.x with pandas fallback.
5. **Marts** (`processing/dbt`): `stg_customers` (view) + `dim_customers` (table)
   built through Trino into `lakehouse.analytics*`.
6. **Orchestration** (Airflow 2.10, LocalExecutor):
   - `lakehouse_cdc` (*/15): bronze → silver → DQ → dbt
   - `lakehouse_batch` (@daily): landing check → silver refresh → hot-tier check
   Both run `spark-submit` against `spark://spark-master:7077` with catalog creds
   injected from the container environment.

## Serving tier
- **Trino** (476, single-node coordinator): `lakehouse` (Iceberg/REST) + `memory`
  catalogs; access control via OPA (`access-control.properties`).
- **StarRocks** (FE 9030/8030 + BE 9050): registered with
  `./lakehouse.sh starrocks-add-be`; create materialized views over the Iceberg
  catalog for concurrent OLAP (runbook).
- **ClickHouse** (24.8): `lakehouse.hot_events` MergeTree with
  `TTL ... TO VOLUME 'cold'` (S3 disk `s3_cold` → Ceph RGW).

## Consumers
- **Superset** 4.1: metadata in shared Postgres `superset` db; connect datasources:
  Trino (`trino://trino:8080/lakehouse`), StarRocks (MySQL dialect,
  `starrocks-fe:9030`), ClickHouse (`clickhouse:9000`).
- **JupyterHub** 5.2 + DockerSpawner: spawns `quay.io/jupyter/scipy-notebook`
  containers on `lakehouse` net with shared volume `jupyter-shared`; env points to
  MLflow (`http://mlflow:5000`), Trino, StarRocks.
- **MLflow** 2.19: backend Postgres `mlflow` db; artifacts on `s3://lake-mlflow`
  through Ceph RGW (`--serve-artifacts`).

## Cross-cutting
- **Keycloak** 26.6: realm `lakehouse` auto-imported; clients pre-registered for
  superset/airflow/nifi/jupyterhub/grafana/openmetadata (+ `lakehouse-cli` direct
  grants). Users: `admin`, `analyst`.
- **OPA** 1.0 (`ops/opa/policies/trino.rego`): `admins_csv` from `.env`
  (`OPA_ADMIN_USERS`) get full access; user `analyst` gets SELECT-only. Trino calls
  `POST /v1/data/trino/allow` per query.
- **Vault** 1.17: file backend; `vault-init` stores Postgres creds at
  `secret/lakehouse/postgres`; root token in `ops/vault/keys/init.json` (git-ignored).
- **OpenMetadata** 1.6 + OpenSearch 2.17: metadata DB in shared Postgres; pipeline
  client points at Airflow REST (`airflow-webserver:8080/api`) for ingestion DAGs.
- **Prometheus/Grafana/Loki/Promtail**: metrics for prometheus/trino/starrocks/
  openmetadata/ceph-rgw; Grafana provisions Prometheus+Loki and a LakeHouse Overview
  dashboard; Promtail ships every `lakehouse` compose container's logs to Loki.
- **HAProxy** edge: host-based routing for all UIs (`*.lakehouse.local` :10080) +
  stats (:18404). **Keepalived** overlay is for two-host VRRP failover (disabled by
  default — pointless on one host).
- **Backup & DR**: `lakehouse.sh backup` = Postgres dumps + volume tarballs +
  S3 upload; `lakehouse.sh restore <ts>` reverses it; runbook covers full rebuild.
