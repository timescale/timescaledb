 #!/bin/bash

# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e

PWD=`pwd`
DIR=`dirname $0`

if [ "$#" -ne 1 ] ; then
    echo "usage: $0 batch_name"
    echo "ex: $0 test_input_data.batch1_dev1"
    exit 1
fi

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_USER=${POSTGRES_USER:-postgres}
INSTALL_DB_MAIN=${INSTALL_DB_MAIN:-Test1}

echo "Connecting to $POSTGRES_HOST as user $POSTGRES_USER and with db $INSTALL_DB_MAIN"

cd $DIR
psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB_MAIN -v ON_ERROR_STOP=1  <<EOF
    SELECT _timescaledb_internal.insert_data((SELECT id FROM _timescaledb_catalog.hypertable h WHERE h.table_name = '33_testNs'), '$1');
EOF
cd $PWD
