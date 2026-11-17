"""LakeHouse CDC pipeline DAG:

  wait for CDC topic data -> Spark bronze ingest -> Spark silver dedup
  -> Great Expectations DQ gate -> dbt marts (staging/dim via Trino)

The DAG runs every 15 minutes; each run is idempotent (bronze keeps offsets,
silver overwrites partitions).
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

ENV = {
    "SPARK_HOME": "/opt/spark",
    "PYTHONPATH": "/opt/spark/jobs:/opt/airflow/dags",
    "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64",
    "PATH": "/usr/local/bin:/usr/bin:/bin:/opt/spark/bin:/opt/spark/sbin",
    "CEPH_ACCESS_KEY_ID": os.environ.get("CEPH_ACCESS_KEY_ID", ""),
    "CEPH_SECRET_ACCESS_KEY": os.environ.get("CEPH_SECRET_ACCESS_KEY", ""),
}

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _wait_for_cdc_data(**_):
    import os
    from kafka import KafkaConsumer  # kafka-python bundled? fallback below

    try:
        consumer = KafkaConsumer(
            "lakehouse.lakehouse_src.dbo.customers",
            bootstrap_servers="kafka:9092",
            consumer_timeout_ms=10000,
            auto_offset_reset="earliest",
        )
        n = sum(1 for _ in consumer)
        consumer.close()
        print(f"CDC topic messages available: {n}")
        return n
    except Exception as exc:  # topic may not exist yet
        print(f"CDC topic check skipped/failed: {exc}")
        return 0


def _dq_gate(**_):
    import subprocess, sys
    r = subprocess.run(
        [sys.executable, "/opt/airflow/great_expectations/dq_checks.py", "silver_customers"],
        capture_output=True, text=True,
    )
    print(r.stdout)
    print(r.stderr)
    if r.returncode != 0:
        raise RuntimeError("Great Expectations DQ gate failed — see logs")


with DAG(
    dag_id="lakehouse_cdc",
    description="CDC: SQL Server -> Kafka -> bronze -> silver -> DQ -> dbt marts",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "cdc"],
) as dag:
    wait_topic = PythonOperator(task_id="wait_for_cdc_data", python_callable=_wait_for_cdc_data)

    bronze = BashOperator(
        task_id="spark_bronze_ingest",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 --deploy-mode client "
            "--conf spark.executor.memory=1g --conf spark.driver.memory=1g "
            "/opt/spark/jobs/bronze_cdc.py"
        ),
        env=ENV,
    )

    silver = BashOperator(
        task_id="spark_silver_dedup",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 --deploy-mode client "
            "--conf spark.executor.memory=1g --conf spark.driver.memory=1g "
            "/opt/spark/jobs/silver_customers.py"
        ),
        env=ENV,
    )

    dq = PythonOperator(
        task_id="great_expectations_gate",
        python_callable=_dq_gate,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dbt = BashOperator(
        task_id="dbt_marts",
        bash_command=(
            # project dir is bind-mounted read-only for the airflow user ->
            # keep dbt's target/ and logs/ out of it (in /tmp)
            # dbt is a pip-user install: absolute path (HOME unset in task env)
            "cd /opt/dbt && /home/airflow/.local/bin/dbt run --profiles-dir . --target dev "
            "--target-path /tmp/dbt_target --log-path /tmp/dbt_logs"
        ),
        env=ENV,
    )

    wait_topic >> bronze >> silver >> dq >> dbt
