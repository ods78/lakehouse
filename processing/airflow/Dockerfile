ARG AIRFLOW_VERSION=2.10.4
FROM apache/airflow:${AIRFLOW_VERSION}

USER root

# JDK for spark-submit (client mode) — DAGs run Spark jobs against the cluster
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

# Full Spark client (spark-submit + jars incl. Iceberg/SDK/Kafka) copied from
# the lakehouse/spark image so DAGs can submit to spark://spark-master:7077
COPY --from=lakehouse/spark:3.5.1 /opt/spark /opt/spark
ENV SPARK_HOME=/opt/spark

USER airflow

# dbt (trino adapter), Great Expectations, trino client, boto3 for S3 checks
# dbt-core MUST match dbt-trino's 1.8.x line — unpinned core (1.12.x) breaks
# profile loading with the 1.8.5 adapter (silent exit 2)
RUN pip install --no-cache-dir \
    "dbt-trino==1.8.5" \
    "dbt-core==1.8.9" \
    "great-expectations==1.9.3" \
    "trino==0.340.0" \
    "boto3" \
    "kafka-python" \
    "psycopg2-binary" \
    "requests"
