#!/bin/bash

# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e

PWD=`pwd`
DIR=`dirname $0`

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_USER=${POSTGRES_USER:-postgres}
INSTALL_DB=${INSTALL_DB:-meta}

if [ "$#" -ne 2 ] ; then
    echo "usage: $0 name host"
    exit 1
fi

NODENAME=$1
NODEHOST=$2

echo "Connecting to $POSTGRES_HOST as user $POSTGRES_USER and with db $INSTALL_DB"
psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB -v ON_ERROR_STOP=1  <<EOF
SELECT set_meta('$NODENAME' :: NAME, '$NODEHOST'::text);
EOF

cd $PWD