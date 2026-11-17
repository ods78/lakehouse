"""Silver transform: deduplicate bronze CDC events -> current customer state.

Run by Airflow DAG lakehouse_cdc after the bronze step.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("lakehouse-silver-customers")
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

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")

# idempotent table bootstrap (fresh catalogs)
spark.sql("""
CREATE TABLE IF NOT EXISTS lakehouse.silver.customers (
    id                  LONG,
    name                STRING,
    email               STRING,
    city                STRING,
    updated_at          TIMESTAMP,
    silver_processed_at TIMESTAMP
) USING iceberg""")

bronze = spark.table("lakehouse.bronze.customers")

w = Window.partitionBy("customer_id").orderBy(F.col("kafka_offset").desc())
latest = (
    bronze.where(F.col("op").isin("r", "c", "u"))
    .withColumn("rk", F.row_number().over(w))
    .where(F.col("rk") == 1)
    .drop("rk")
    .select(
        F.col("customer_id").alias("id"),
        "name", "email", "city",
        F.to_timestamp("updated_at").alias("updated_at"),
        F.current_timestamp().alias("silver_processed_at"),
    )
)

latest.writeTo("lakehouse.silver.customers").overwritePartitions()
print(f"SILVER: lakehouse.silver.customers now holds {latest.count()} current customers")
spark.stop()
