"""LakeHouse daily batch DAG:

  check landing zone files in Ceph S3 -> Spark silver customers refresh
  -> ClickHouse hot-tier check

Add NiFi process groups that copy API/file payloads into
s3://lake-landing/<source>/<date>/; this DAG reacts to new landings.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

ENV = {
    "SPARK_HOME": "/opt/spark",
    "PYTHONPATH": "/opt/spark/jobs:/opt/airflow/dags",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/opt/spark/bin:/opt/spark/sbin",
    "CEPH_ACCESS_KEY_ID": os.environ.get("CEPH_ACCESS_KEY_ID", ""),
    "CEPH_SECRET_ACCESS_KEY": os.environ.get("CEPH_SECRET_ACCESS_KEY", ""),
    "CLICKHOUSE_USER": os.environ.get("CLICKHOUSE_USER", "dwh"),
    "CLICKHOUSE_PASSWORD": os.environ.get("CLICKHOUSE_PASSWORD", ""),
}

default_args = {"owner": "lakehouse", "retries": 1, "retry_delay": timedelta(minutes=2)}


def _check_landing(**_):
    import boto3, os

    s3 = boto3.client(
        "s3",
        endpoint_url="http://ceph-rgw:8080",
        aws_access_key_id=os.environ.get("CEPH_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.environ.get("CEPH_SECRET_ACCESS_KEY", ""),
        region_name="us-east-1",
    )
    resp = s3.list_objects_v2(Bucket="lake-landing", MaxKeys=5)
    n = resp.get("KeyCount", 0)
    print(f"Landing zone objects (sample): {n}")
    if n:
        for o in resp.get("Contents", [])[:5]:
            print(f"  {o['Key']} ({o['Size']} bytes)")
    return n


with DAG(
    dag_id="lakehouse_batch",
    description="Landing-zone aware daily batch refresh",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "batch"],
) as dag:
    landing = PythonOperator(task_id="check_landing_zone", python_callable=_check_landing)

    silver = BashOperator(
        task_id="spark_silver_refresh",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 --deploy-mode client "
            "--conf spark.executor.memory=1g --conf spark.driver.memory=1g "
            "/opt/spark/jobs/silver_customers.py"
        ),
        env=ENV,
    )

    hot_tier = BashOperator(
        task_id="clickhouse_hot_tier_check",
        bash_command=(
            "curl -s 'http://clickhouse:8123/?user=${CLICKHOUSE_USER}&password=${CLICKHOUSE_PASSWORD}' "
            "--data-binary \"SELECT event_type, count() FROM lakehouse.hot_events GROUP BY event_type\" "
            "|| echo 'clickhouse hot tier check failed (non-fatal)'"
        ),
        env=ENV,
    )

    landing >> silver >> hot_tier
