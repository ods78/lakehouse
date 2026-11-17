# apache/superset:4.1.1 ships without the Postgres driver — the metadata DB
# (superset.db / Postgres) connection needs psycopg2
FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.9
USER superset
