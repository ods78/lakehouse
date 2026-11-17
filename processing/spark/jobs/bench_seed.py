"""Load the Phase-1 benchmark dataset (benchmarks/data/*.csv) into Iceberg so
Trino queries the identical rows ClickHouse has in bench.*.
Run:  spark-submit --master spark://spark-master:7077 /opt/spark/jobs/bench_seed.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import types as T

DATA = "/opt/spark/jobs/bench_data"

spark = (
    SparkSession.builder.appName("lakehouse-bench-seed")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bench")
spark.sql("DROP TABLE IF EXISTS lakehouse.bench.customer")
spark.sql("DROP TABLE IF EXISTS lakehouse.bench.product")
spark.sql("DROP TABLE IF EXISTS lakehouse.bench.lineorder")

customer_schema = T.StructType([
    T.StructField("c_id", T.IntegerType()), T.StructField("c_name", T.StringType()),
    T.StructField("c_region", T.StringType()), T.StructField("c_nation", T.StringType()),
    T.StructField("c_mktsegment", T.StringType()),
])
product_schema = T.StructType([
    T.StructField("p_id", T.IntegerType()), T.StructField("p_name", T.StringType()),
    T.StructField("p_brand", T.StringType()), T.StructField("p_category", T.StringType()),
    T.StructField("p_size", T.IntegerType()),
])
lineorder_schema = T.StructType([
    T.StructField("lo_id", T.LongType()), T.StructField("lo_custkey", T.IntegerType()),
    T.StructField("lo_prodkey", T.IntegerType()), T.StructField("lo_quantity", T.IntegerType()),
    T.StructField("lo_discount", T.IntegerType()), T.StructField("lo_price", T.IntegerType()),
    T.StructField("lo_datekey", T.DateType()), T.StructField("lo_year", T.IntegerType()),
])

for name, schema in [("customer", customer_schema), ("product", product_schema), ("lineorder", lineorder_schema)]:
    df = (
        spark.read.schema(schema)
        .option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .csv(f"{DATA}/{name}.csv")
    )
    df.writeTo(f"lakehouse.bench.{name}").create()
    print(f"BENCH SEED: lakehouse.bench.{name} rows={spark.table(f'lakehouse.bench.{name}').count()}")

spark.stop()
