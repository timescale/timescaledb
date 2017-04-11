#!/bin/bash

# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e

PWD=`pwd`
DIR=`dirname $0`

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_PORT=${POSTGRES_PORT:-5432}
POSTGRES_USER=${POSTGRES_USER:-postgres}
INSTALL_DB=${INSTALL_DB:-meta}

if [ "$#" -ne 2 ] ; then
    echo "usage: $0 node host"
    exit 1
fi

NODENAME=$1
NODEHOST=$2
NODEPORT=$3

echo "Connecting to $POSTGRES_HOST as user $POSTGRES_USER and with db $INSTALL_DB"
psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB -X -v ON_ERROR_STOP=1  <<EOF
SELECT add_node('$NODENAME' :: NAME, '$NODEHOST', $NODEPORT);
EOF

cd $PWD