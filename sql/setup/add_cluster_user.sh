#!/bin/bash

# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e

PWD=`pwd`
DIR=`dirname $0`

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_USER=${POSTGRES_USER:-postgres}
INSTALL_DB=${INSTALL_DB:-meta}

if [[ "$#" -eq 0 || "$#" -gt 2 ]] ; then
    echo "usage: $0 user [pass]"
    exit 1
fi

if [ "$#" == 2 ] ; then
    PASS=$2
else
    PASS="NULL"
fi

PG_USER_TO_ADD=$1

echo "Connecting to $POSTGRES_HOST as user $POSTGRES_USER and with db $INSTALL_DB"
echo "SELECT add_cluster_user('$PG_USER_TO_ADD', $PASS);"
cd $DIR
psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB -v ON_ERROR_STOP=1  <<EOF
SELECT add_cluster_user('$PG_USER_TO_ADD', $PASS);
EOF

cd $PWD