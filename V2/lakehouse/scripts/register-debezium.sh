#!/bin/bash
set -e; cd "$(dirname "$0")/.."; set -a; . ./.env; set +a
until curl -sf http://localhost:8083/ >/dev/null; do echo "waiting for connect"; sleep 3; done
curl -sf -X PUT -H 'Content-Type: application/json' http://localhost:8083/connectors/mssql-shop/config -d @- <<JSON
{"connector.class":"io.debezium.connector.sqlserver.SqlServerConnector",
 "database.hostname":"mssql","database.port":"1433","database.user":"sa","database.password":"$MSSQL_SA_PASSWORD",
 "database.names":"shop","topic.prefix":"cdc","table.include.list":"dbo.customers,dbo.orders",
 "database.encrypt":"false",
 "schema.history.internal.kafka.bootstrap.servers":"kafka:9092","schema.history.internal.kafka.topic":"cdc.schema-history"}
JSON
echo; echo "connector registered"
