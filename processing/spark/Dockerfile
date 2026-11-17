FROM apache/spark:3.5.1

USER root

# Iceberg (Spark runtime) + Iceberg-AWS (S3FileIO) + AWS SDK v2 bundle
# (the 1.6.0 aws-bundle ships only the shaded SDK — the S3FileIO class lives
# in the separate iceberg-aws artifact), plus Kafka connector jars.
# Jars are vendored in jars-lib/ so the build never hits the network.
COPY jars-lib/*.jar /opt/spark/jars/
RUN chmod 644 /opt/spark/jars/*.jar

ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/spark/bin:$PATH
