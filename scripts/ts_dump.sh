#!/usr/bin/env bash

#  This file and its contents are licensed under the Apache License 2.0.
#  Please see the included NOTICE for copyright information and
#  LICENSE-APACHE for a copy of the license.

# This script is used for backing up a single hypertable into an easy-to-restore
# tarball. The tarball contains two files: (1) a .sql file for recreating the
# hypertable and its indices and (2) a .csv file containing the data as CSV.
#
# Because pg_dump/pg_restore dump all of TimescaleDB's internal tables when
# used, this script is useful if you want a backup that can be restored
# regardless of TimescaleDB version running, or as part of a process where you
# do not want to always backup all your hypertables at once.


if [[ -z "$1" || -z "$2" ]]; then
    echo "Usage: $0 hypertable output_name [pg_dump CONNECTION OPTIONS]"
    echo "    hypertable  - Hypertable to backup"
    echo "    output_name - Output files will be stored in an archive named [output_name].tar.gz"
    echo "    "
    echo "Any connection options for pg_dump/psql (e.g. -d database, -U username) should be listed at the end"
    exit 1
fi

HYPERTABLE=$1
PREFIX=$2

shift 2
set -e
echo "Backing up schema as $PREFIX-schema.sql..."
pg_dump "$@" --schema-only -t $HYPERTABLE -f $PREFIX-schema.sql
echo >> $PREFIX-schema.sql "--
-- Restore to hypertable
--"
psql "$@" -qAtX -c "SELECT _timescaledb_internal.get_create_command('$HYPERTABLE');" >> $PREFIX-schema.sql

echo "Backing up data as $PREFIX-data.csv..."
psql "$@" -c "\COPY (SELECT * FROM $HYPERTABLE) TO $PREFIX-data.csv DELIMITER ',' CSV"

echo "Archiving and removing previous files..."
tar -czvf $PREFIX.tar.gz $PREFIX-data.csv $PREFIX-schema.sql
rm $PREFIX-data.csv
rm $PREFIX-schema.sql
