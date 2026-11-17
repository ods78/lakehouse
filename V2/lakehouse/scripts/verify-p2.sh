#!/bin/bash
# P2: SQL Server row change -> Kafka message. NiFi->S3 check needs the flow built once (GetFile /opt/nifi/fixtures -> PutS3Object).
set -e; cd "$(dirname "$0")/.."; set -a; . ./.env; set +a
./scripts/register-debezium.sh; sleep 5
docker compose exec -T mssql /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -d shop -Q "UPDATE dbo.orders SET status='shipped' WHERE id=1"
echo "--- last message on cdc.shop.dbo.orders:"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic cdc.shop.dbo.orders --from-beginning --timeout-ms 20000 2>/dev/null | tail -n 1 | cut -c1-300
echo "--- S3 landing objects:"
docker run --rm --network lh_storage -e AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" -e AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY" -e AWS_DEFAULT_REGION="$S3_REGION" amazon/aws-cli --endpoint-url "$S3_ENDPOINT" s3 ls "s3://$S3_BUCKET/landing/" || true
