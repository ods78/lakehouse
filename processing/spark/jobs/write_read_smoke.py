"""Smoke test: write + read an Iceberg table through the REST catalog on Ceph S3.

Run: ./lakehouse.sh smoke-spark
"""
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("lakehouse-smoke")
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
    .config("spark.sql.catalog.lakehouse.s3.access-key-id", __import__("os").environ.get("CEPH_ACCESS_KEY_ID", ""))
    .config("spark.sql.catalog.lakehouse.s3.secret-access-key", __import__("os").environ.get("CEPH_SECRET_ACCESS_KEY", ""))
    .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")

rows = [(1, "smoke-row-1"), (2, "smoke-row-2")]
df = spark.createDataFrame(rows, "id INT, note STRING")

df.writeTo("lakehouse.bronze.smoke_test").using("iceberg").createOrReplace()

result = spark.table("lakehouse.bronze.smoke_test").collect()
assert [r["id"] for r in result] == [1, 2], f"unexpected read-back: {result}"

# Iceberg metadata sanity: snapshots must exist
snaps = spark.sql("SELECT snapshot_id FROM lakehouse.bronze.smoke_test.snapshots").count()
assert snaps >= 1, "no snapshots recorded — ACID metadata missing"

print(f"SMOKE OK: lakehouse.bronze.smoke_test rows={len(result)} snapshots={snaps}")
spark.stop()
