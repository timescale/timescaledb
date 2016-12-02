#!/bin/bash

#
# NOTE ! This script will not populate the db. You have to make sure the right data is imported before the query
#
set -u
set -e

PWD=`pwd`
DIR=`dirname $0`

POSTGRES_HOST=${POSTGRES_HOST:-localhost}
POSTGRES_USER=${POSTGRES_USER:-postgres}
QUERY_DB=${QUERY_DB:-Test1}

if [ "$#" -ne 2 ] ; then
    echo "usage: $0 query outputfile"
    echo "ex: $0 \"new_ioql_query(namespace_name => '33_testNs')\" result.csv "
    echo "NOTE ! This script will not populate the db. You have to make sure the right data is imported before the query"
    exit 1
fi

QUERY=$1
OUTPUTFILE=$2
TMPFILE="output.tmp"

echo "Connecting to $POSTGRES_HOST as user $POSTGRES_USER and with db $QUERY_DB"
psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $QUERY_DB -v ON_ERROR_STOP=1  <<EOF
CREATE SCHEMA IF NOT EXISTS tmp;
DROP TABLE IF EXISTS tmptable;
select test_utils.query_to_table($QUERY, 'tmptable');
\copy (Select * From tmptable) To '$TMPFILE' With CSV DELIMITER ';' NULL 'null';
EOF

echo "select test_utils.query_to_table($QUERY, 'tmptable');"

#
# Convert
#
#             Table "public.tmptable"
#     Column      |       Type       | Modifiers
# -----------------+------------------+-----------
# time            | bigint           |
# bool_1          | boolean          |
# ....
#
# into
#
# time bigint ; bool_1 boolean;
#
# will also change parenthesis to underscores.

psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $QUERY_DB -v ON_ERROR_STOP=1 -c "\d tmptable" | \
tail -n +4 | sed "s/|//;s/|/;/;s/  */ /g;s/ ; /;/g" | tr -d '\n' | sed "s/.$//;s/^ //;s/[\(,\)]/_/g" > $OUTPUTFILE

# add the rows from the query to the file
cat $TMPFILE >> $OUTPUTFILE
rm $TMPFILE

cd $PWD