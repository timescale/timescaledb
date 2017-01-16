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
  psql -U $POSTGRES_USER -h $POSTGRES_HOST -d $INSTALL_DB_MAIN -v ON_ERROR_STOP=1  <<EOF
CREATE TABLE PUBLIC."$NAMESPACE" (
  time BIGINT NOT NULL,
  bool_1 BOOLEAN NULL,
  device_id TEXT NOT NULL,
  field_only_dev2 DOUBLE PRECISION NULL,
  field_only_ref2 TEXT NULL,
  "nUm_1" DOUBLE PRECISION NULL,
  num_2 DOUBLE PRECISION NULL,
  string_1 TEXT NULL,
  string_2 TEXT NULL
);

CREATE INDEX ON PUBLIC."$NAMESPACE" (time, device_id);
CREATE INDEX ON PUBLIC."$NAMESPACE" (device_id, time);
CREATE INDEX ON PUBLIC."$NAMESPACE" ("nUm_1", time);

SELECT * FROM create_hypertable('"public"."$NAMESPACE"', 'time', 'device_id', hypertable_name=>'$NAMESPACE');

SELECT set_is_distinct_flag('"public"."$NAMESPACE"', 'device_id', TRUE);
SELECT set_is_distinct_flag('"public"."$NAMESPACE"', 'string_1', TRUE);
SELECT set_is_distinct_flag('"public"."$NAMESPACE"', 'string_2', TRUE);

EOF

done

cd $PWD
