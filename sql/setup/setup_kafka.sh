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
\c $INSTALL_DB
\ir ../main/kafka_offset_table.sql
\ir ../main/kafka_offset_node_trigger.sql
\ir ../main/kafka_offset_functions.sql
EOF

cd $PWD