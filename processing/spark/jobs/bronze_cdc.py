"""Bronze ingest: Debezium JSON CDC events (Kafka topic lakehouse.lakehouse_src.dbo.customers)
-> append-only Iceberg bronze table.

Run by Airflow DAG lakehouse_cdc (idempotent: re-reading from earliest offsets is
safe because bronze keeps every raw event with its offset).
"""
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType,
)

TOPIC = os.environ.get("CDC_TOPIC", "lakehouse.lakehouse_src.dbo.customers")

# Debezium JsonConverter runs with schemas.enable=true, so each message is
# {"schema": {...}, "payload": {...}} — we parse the payload struct.
# DATETIME2 arrives as nanos-since-epoch (LongType); converted below.
_CUSTOMER_STRUCT = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("updated_at", LongType()),
])
DEBEZIUM_VALUE_SCHEMA = StructType([
    StructField("before", _CUSTOMER_STRUCT, True),
    StructField("after", _CUSTOMER_STRUCT, True),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
])

spark = (
    SparkSession.builder.appName("lakehouse-bronze-cdc")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3://lake-iceberg/warehouse")
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://ceph-rgw:8080")
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.catalog.lakehouse.s3.access-key-id", os.environ.get("CEPH_ACCESS_KEY_ID", ""))
    .config("spark.sql.catalog.lakehouse.s3.secret-access-key", os.environ.get("CEPH_SECRET_ACCESS_KEY", ""))
    .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

# idempotent table bootstrap (fresh catalogs). updated_at is converted from
# Debezium nanos to a real timestamp; ts_ms kept as raw epoch-millis.
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.bronze.customers (
    kafka_offset LONG,
    op            STRING,
    ts_ms         LONG,
    customer_id   LONG,
    name          STRING,
    email         STRING,
    city          STRING,
    updated_at    TIMESTAMP,
    ingested_at   TIMESTAMP
) USING iceberg""")

raw = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 100000)
    .load()
)

if raw.isEmpty():
    print(f"BRONZE: topic {TOPIC} is empty — nothing to ingest")
    spark.stop()
    raise SystemExit(0)

events = (
    raw.select(F.col("offset").cast("long").alias("kafka_offset"),
               F.from_json(F.col("value").cast("string"),
                           StructType([StructField("payload", DEBEZIUM_VALUE_SCHEMA)])).getField("payload").alias("ev"))
    .select(
        "kafka_offset",
        F.col("ev.op").alias("op"),
        F.col("ev.ts_ms").alias("ts_ms"),
        F.coalesce(F.col("ev.after.id"), F.col("ev.before.id")).alias("customer_id"),
        F.coalesce(F.col("ev.after.name"), F.col("ev.before.name")).alias("name"),
        F.coalesce(F.col("ev.after.email"), F.col("ev.before.email")).alias("email"),
        F.coalesce(F.col("ev.after.city"), F.col("ev.before.city")).alias("city"),
        F.coalesce(F.col("ev.after.updated_at"), F.col("ev.before.updated_at"))
            .cast("double").alias("updated_at_nanos"),
    )
    .withColumn("updated_at", F.timestamp_seconds(F.col("updated_at_nanos") / F.lit(1e9)))
    .drop("updated_at_nanos")
    .withColumn("ingested_at", F.current_timestamp())
)

existing = spark.table("lakehouse.bronze.customers")
seen = existing.agg(F.max("kafka_offset").alias("max_off")).collect()[0]["max_off"]
if seen is not None:
    events = events.where(F.col("kafka_offset") > F.lit(seen))

if events.isEmpty():
    print("BRONZE: no new events beyond stored offsets")
else:
    events.writeTo("lakehouse.bronze.customers").append()
    print(f"BRONZE: appended {events.count()} CDC events")
spark.stop()
