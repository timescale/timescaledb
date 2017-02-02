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
CREATE EXTENSION IF NOT EXISTS iobeamdb CASCADE;

\o /dev/null
\echo 'Set up database as meta node...'
select setup_meta();
\echo 'Set up database as data node...'
select setup_main();

SELECT add_cluster_user('postgres', NULL);

\echo 'Adding database iobeam to the single-node cluster...'
SELECT set_meta('${DB_NAME}' :: NAME, 'localhost');
SELECT add_node('${DB_NAME}' :: NAME, 'localhost');

\echo 'Success'
EOF
