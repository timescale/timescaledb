#!/usr/bin/env bash

# Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
#
# This file is licensed under the Apache License, see LICENSE-APACHE
# at the top level directory of the timescaledb distribution.

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
psql -q -v "ON_ERROR_STOP=1" $@ < $PREFIX-schema.sql
if [ $? -ne 0 ]; then
    echo "Restoring schema failed, exiting."
    rm $PREFIX-data.csv
    rm $PREFIX-schema.sql
    exit $?
fi

echo "Restoring hypertable's data..."
psql $@ -v "ON_ERROR_STOP=1" -c "\COPY $HYPERTABLE FROM $PREFIX-data.csv DELIMITER ',' CSV"
if [ $? -ne 0 ]; then
    echo "Restoring data failed, exiting."
fi

rm $PREFIX-data.csv
rm $PREFIX-schema.sql
