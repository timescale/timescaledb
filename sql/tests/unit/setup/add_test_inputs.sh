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

# Todo - read the ns and fields from the csv/tsv file
NAMESPACES="33_testNs emptyNs"
for NAMESPACE in $NAMESPACES; do
  psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB_META -v ON_ERROR_STOP=1  <<EOF
      SELECT add_hypertable('$NAMESPACE' :: NAME, 'device_id');
      SELECT add_field('$NAMESPACE' :: NAME, 'device_id', 'text', TRUE, TRUE, ARRAY ['VALUE-TIME'] :: field_index_type []);
      SELECT add_field('$NAMESPACE' :: NAME, 'nUm_1', 'double precision', FALSE, FALSE, ARRAY ['VALUE-TIME'] :: field_index_type []);
      SELECT add_field('$NAMESPACE' :: NAME, 'num_2', 'double precision', FALSE, FALSE, ARRAY ['VALUE-TIME'] :: field_index_type []);
      SELECT add_field('$NAMESPACE' :: NAME, 'bool_1', 'boolean', FALSE, FALSE, ARRAY ['VALUE-TIME'] :: field_index_type []);
      SELECT add_field('$NAMESPACE' :: NAME, 'string_1', 'text', FALSE, FALSE, ARRAY ['VALUE-TIME'] :: field_index_type []);
      SELECT add_field('$NAMESPACE' :: NAME, 'string_2', 'text', FALSE, FALSE, ARRAY ['VALUE-TIME'] :: field_index_type []);
      SELECT add_field('$NAMESPACE' :: NAME, 'field_only_ref2', 'text', FALSE, FALSE, ARRAY ['VALUE-TIME'] :: field_index_type []);
      SELECT add_field('$NAMESPACE' :: NAME, 'field_only_dev2', 'double precision', FALSE, FALSE, ARRAY ['VALUE-TIME'] :: field_index_type []);
EOF

done

INPUT_DATA_DIR="../import_data" 
FILE_SUFFIX=".tsv"
DATASETS=`ls $INPUT_DATA_DIR/*$FILE_SUFFIX`
TEMPTABLENAME="copy_t2"

for DS_PATH in $DATASETS; do 
    DATASET=`basename $DS_PATH $FILE_SUFFIX`
    PARTITION_KEY=`echo $DATASET | cut -f2 -d_ `
    echo "Setting up $DATASET with partitionkey $PARTITION_KEY"
 
psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB_MAIN -v ON_ERROR_STOP=1  <<EOF
        BEGIN;
        DROP TABLE IF EXISTS $TEMPTABLENAME;
        SELECT *
        FROM create_temp_copy_table('$TEMPTABLENAME');
        \COPY $TEMPTABLENAME FROM '$DS_PATH';
        CREATE SCHEMA IF NOT EXISTS test_input_data;
        DROP TABLE IF EXISTS test_input_data.$DATASET;
        CREATE TABLE test_input_data.$DATASET AS SELECT * FROM $TEMPTABLENAME; 
        COMMIT; 
EOF

done  

cd $PWD

 
