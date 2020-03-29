#!/usr/bin/env bash

: "${POSTGRES_HOST:="postgres"}"
: "${POSTGRES_PORT:="5432"}"
: "${POSTGRES_USER:="airflow"}"
: "${POSTGRES_PASSWORD:="airflow"}"
: "${POSTGRES_DB:="airflow"}"

# Install custom python package if requirements.txt is present
if [ -e "/requirements.txt" ]; then
    $(which pip) install --user -r /requirements.txt
fi

TRY_LOOP="20"

wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"

# Create connection
#export AIRFLOW_CONN_MY_PROD_DATABASE='postgres://test:postgres@postgres:5432/test'

# Initialize airflow db (LocalExecutor)
airflow initdb

# Initialize scheduler
airflow scheduler

# Initialize webserver
airflow webserver

# Create connection
# airflow connections add --conn_id 'postgres_test' --conn_uri 'postgres://test:postgres@postgres:5432/test'