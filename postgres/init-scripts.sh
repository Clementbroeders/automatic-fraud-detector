#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE mlflow;
    CREATE DATABASE fraud;
    CREATE DATABASE airflow;
    CREATE ROLE airflow WITH LOGIN PASSWORD 'airflow';
    ALTER ROLE airflow CREATEDB;
    ALTER ROLE airflow CREATEROLE;
EOSQL