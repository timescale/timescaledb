#!/usr/bin/env bash

#  This file and its contents are licensed under the Apache License 2.0.
#  Please see the included NOTICE for copyright information and
#  LICENSE-APACHE for a copy of the license.

# This script is used for restoring a hypertable from a tarball made with
# ts_dump.sh. It unarchives the tarball, producing a schema file and data file,
# which are then restore separately.


if [[ -z "$1" || -z "$2" ]]; then
    echo "Usage: $0 hypertable tarfile_name"
    echo "    hypertable   - Hypertable to restore"
    echo "    tarfile_name - Name of the tarball created by ts_dump.sh to restore"
    echo "    "
    echo "Any connection options for pg_restore/psql (e.g. -d database, -U username) should be listed at the end"
    exit 1

fi

HYPERTABLE=$1
TARFILE=$2
PREFIX="${TARFILE/.tar.gz/}"
shift 2
echo "Unarchiving tarball..."
tar xvzf $TARFILE

echo "Restoring hypertable's schema..."
if ! psql -q -v "ON_ERROR_STOP=1" "$@" < $PREFIX-schema.sql
then
    echo "Restoring schema failed, exiting."
    rm $PREFIX-data.csv
    rm $PREFIX-schema.sql
    exit $?
fi

echo "Restoring hypertable's data..."
if ! psql "$@" -v "ON_ERROR_STOP=1" -c "\COPY $HYPERTABLE FROM $PREFIX-data.csv DELIMITER ',' CSV"
then
    echo "Restoring data failed, exiting."
fi

rm $PREFIX-data.csv
rm $PREFIX-schema.sql
