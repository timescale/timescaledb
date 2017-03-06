#!/bin/bash

# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e

PWD=`pwd`
DIR=`dirname $0`

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_USER=${POSTGRES_USER:-postgres}


if [ "$#" -ne 1 ]; then
    echo "usage: $0 nodename"
    exit 1
fi

INSTALL_DB=$1

echo "Connecting to $POSTGRES_HOST as user $POSTGRES_USER and with db $INSTALL_DB"

cd $DIR
psql -U $POSTGRES_USER -h $POSTGRES_HOST -v ON_ERROR_STOP=1  <<EOF
DROP DATABASE IF EXISTS "$INSTALL_DB";
CREATE DATABASE "$INSTALL_DB";
\c "$INSTALL_DB"
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
SELECT _timescaledb_internal.setup_main();
EOF

cd $PWD
