 #!/bin/bash

# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e

PWD=`pwd`
DIR=`dirname $0`

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_USER=${POSTGRES_USER:-postgres}
INSTALL_DB_META=${INSTALL_DB_META:-meta}
INSTALL_DB_MAIN=${INSTALL_DB_MAIN:-Test1}

echo "Connecting to $POSTGRES_HOST as user $POSTGRES_USER and with meta db $INSTALL_DB_META and main db $INSTALL_DB_MAIN"

cd $DIR

OUTPUT_DATA_DIR="expected_outputs"
FILE_SUFFIX="csv"
DATAFILES=`ls $OUTPUT_DATA_DIR/*.$FILE_SUFFIX`
SCHEMA_NAME="test_outputs"
DELIMITER=";"
TEMPFILE="tempfile.tmp"

for DS_PATH in $DATAFILES; do 
    DATASET=`basename $DS_PATH .$FILE_SUFFIX `
    echo "Setting up output $DATASET"

    COLUMNS=`head -n 1 $DS_PATH | sed "s/$DELIMITER/,/g"`
    tail -n +2 $DS_PATH > $TEMPFILE

psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB_MAIN -v ON_ERROR_STOP=1  <<EOF
        BEGIN;
        CREATE SCHEMA IF NOT EXISTS $SCHEMA_NAME;
        DROP TABLE IF EXISTS $SCHEMA_NAME.$DATASET;
        CREATE TABLE $SCHEMA_NAME.$DATASET ($COLUMNS);
        \COPY $SCHEMA_NAME.$DATASET FROM '$TEMPFILE' DELIMITER '$DELIMITER' NULL 'null';
        COMMIT; 
EOF

done  

rm $TEMPFILE
cd $PWD
 
 