#!/usr/bin/env bash
set -euo pipefail

service postgresql start
runuser -u postgres -- psql --set ON_ERROR_STOP=1 \
  --command "ALTER USER postgres PASSWORD 'postgres'"
if ! runuser -u postgres -- psql --tuples-only --command \
  "SELECT 1 FROM pg_database WHERE datname = 'veris'" | grep -q 1; then
  runuser -u postgres -- createdb veris
  runuser -u postgres -- psql --set ON_ERROR_STOP=1 \
    --dbname veris --file /agent/db/init.sql
fi

exec /agent/.venv/bin/python -m app.main
