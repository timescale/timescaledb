#!/bin/bash

set -e

if [[ -z "$DB_NAME" ]]; then
  echo "The DB_NAME must be set"
  exit 1
fi

# Create data directories for tablespaces tests
psql -h localhost -U postgres -v ON_ERROR_STOP=1 << EOF
\echo 'Creating database: ${DB_NAME}'
CREATE DATABASE ${DB_NAME};

\c ${DB_NAME}
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

EOF
