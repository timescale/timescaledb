#!/bin/bash
# This script allows one to insert a large CSV file by splitting it into
# smaller batches. We do this in order to not bypass TimescaleDB's
# chunk mechanism which currently does not close a chunk mid-insert even
# if the insert would over-fill the chunk.

if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ]; then
   echo "Usage: $0 csv_file db_name table_name"
   exit 1
fi

# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e

export PGUSER=${PGUSER:-postgres}
export PGHOST=${PGHOST:-localhost}

# Remove any previous split files
rm -f .timescaledb_temp_*

echo "Splitting CSV into batches of 500,000 rows..."
split -l 500000 $1 .timescaledb_temp_
echo "[OK]"

echo "Importing data..."
for f in .timescaledb_temp_*; do
    tempstr="\COPY \"$3\" FROM $f CSV"
    psql -v ON_ERROR_STOP=1 -X -d $2 -c ''"$tempstr"''
done
echo "[OK]"

echo "Cleaning up..."
rm -f .timescaledb_temp_*
echo "[OK]"
