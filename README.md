# LakeHouse

A single-host Docker Lakehouse implementing the full architecture diagram:
**Sources → Ingestion & Contracts → Storage & Hot Tier → Processing & Orchestration
→ Table Format & Catalog → Serving → Consumers**, with cross-cutting ops.

## Quick start

```bash
cd ~/Dockers/LakeHouse
./lakehouse.sh init              # .env + rendered configs + image builds
./lakehouse.sh up all            # start everything (builds/pulls on first run)
./lakehouse.sh bootstrap-ceph    # once: S3 user + buckets (waits for HEALTH_OK)
./lakehouse.sh seed-mssql        # once: CDC setup + sample rows
./lakehouse.sh register-cdc      # once: Debezium connector
./lakehouse.sh starrocks-add-be  # once: register BE with FE
./lakehouse.sh vault-init        # once: init/unseal Vault
./lakehouse.sh smoke             # end-to-end integration check
./lakehouse.sh urls              # print all UI endpoints + creds
```

Control commands:
```bash
./lakehouse.sh up [phase|all]    # base sources ingestion storage catalog
                                 # processing serving consumers ops ops-ha
./lakehouse.sh down|stop|restart [phase|all]
./lakehouse.sh status | logs [service] | build | pull
./lakehouse.sh backup | restore <ts> | reset | hosts
```

Incremental (phase by phase, dependency order):
`storage → bootstrap-ceph → sources → ingestion → register-cdc → catalog →
processing → serving → starrocks-add-be → consumers → ops`

## Layers & services

| Phase | Layer | Services |
|---|---|---|
| 0 | Foundation | shared PostgreSQL (7 DBs), `lakehouse` network 172.31.100.0/24 |
| 1 | Sources | SQL Server 2022 (CDC-enabled `lakehouse_src.dbo.customers`) |
| 2 | Ingestion & contracts | Kafka 3.9 (KRaft), Schema Registry 7.7, Debezium 2.7, NiFi 1.28, Kafka UI |
| 3 | Storage & hot tier | Ceph MON+MGR+OSD+RGW (S3), ClickHouse 24.8 (TTL → S3 cold volume) |
| 4 | Processing & orchestration | Spark 3.5 (Iceberg+Kafka jars), Airflow 2.10 (LocalExecutor), dbt-trino, Great Expectations |
| 5 | Table format & catalog | Iceberg REST catalog 1.6 (JDBC/Postgres backend, S3FileIO → Ceph) |
| 6 | Serving | Trino 476 (REST catalog + native S3), StarRocks 3.3 (FE+BE) |
| 7 | Consumers | Superset 4.1, JupyterHub 5.2 (+DockerSpawner), MLflow 2.19 (S3 artifacts) |
| 8 | Cross-cutting | Vault 1.17, Keycloak 26.6 (SSO + realm import), OpenMetadata 1.6 (+OpenSearch), Prometheus/Grafana/Loki/Promtail, OPA (Trino access control), HAProxy edge (+ optional Keepalived for multi-host) |

## Data flow (how it maps to the diagram)

```
SQL Server --CDC(Debezium)--> Kafka --bronze job--> Iceberg bronze (Ceph S3)
NiFi --batch/files/APIs------> s3://lake-landing  -+
                                                  v
                        Spark silver (dedup) -> Iceberg silver (Ceph S3)
                                                  |
                          Great Expectations DQ gate (via Trino)
                                                  v
                        dbt marts (staging/dim via Trino)
                                                  |
        +-----------------+----------------------+
        v                 v                      v
    Trino 476       StarRocks FE/BE        ClickHouse hot tier (TTL -> S3)
        ^                 ^                      ^
        +-------- Superset (BI) ----------------+
                     + JupyterHub/MLflow (artifacts on Ceph)
```

## Ports

Avoids your other stacks (5050/5433/6333/8082/8088/9443/27017). See `.env` §Host ports
or run `./lakehouse.sh urls`.

## Operations

- `./lakehouse.sh status | logs <service> | smoke | backup | restore <ts> | reset`
- Backup: Postgres dumps + named-volume archives + upload to `s3://lake-backups` (see `docs/runbook.md`).
- HAProxy edge routes `*.lakehouse.local` (`./lakehouse.sh hosts`, needs sudo).
- Keepalived: only meaningful with a second host — `./lakehouse.sh up ops-ha`.

## Documentation

- `docs/architecture.md` — layer map & integration contract per service
- `docs/runbook.md` — per-service ops, SSO wiring, DR procedures
