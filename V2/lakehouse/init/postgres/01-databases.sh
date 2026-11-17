#!/bin/bash
# Runs once on an empty volume. One DB + role per consumer; password from PG_<NAME>_PASSWORD or POSTGRES_PASSWORD.
set -e
for d in iceberg airflow keycloak superset nifi openmetadata; do
  var="PG_${d^^}_PASSWORD"; pw="${!var:-$POSTGRES_PASSWORD}"
  psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" <<SQL
CREATE USER $d WITH PASSWORD '$pw';
CREATE DATABASE $d OWNER $d;
SQL
done
