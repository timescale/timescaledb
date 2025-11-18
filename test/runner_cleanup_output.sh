#!/usr/bin/env bash

set -u
set -e

RUNNER=${1:-""}

sed  -E -e '/<exclude_from_test>/,/<\/exclude_from_test>/d' \
     -e 's! Disk: [0-9]+kB!!' \
     -e 's! Memory: [0-9]+kB!!' \
     -e 's! Memory Usage: [0-9]+kB!!' \
     -e 's! Average  Peak Memory: [0-9]+kB!!' \
     -e 's!ERROR:  permission denied for materialized view!ERROR:  permission denied for view!' \
     -e 's!"*_ts_meta_v2_bl[0-9A-Za-z]+_([_0-9A-Za-z]+)"*!_ts_meta_v2_bloomXXX_\1!g' \
	 -e 's/(actual rows=[0-9]+) /\1.00 /' \
	 -e '/^ ?\([0-9]+ row[s]?\)$/d' \
	 -e '/ +QUERY PLAN +/{N;s/ +QUERY PLAN +\n-+/--- QUERY PLAN ---/;}' \
     -e '/Disabled: true/d' \
     -e '/Heap Fetches: [0-9]+/d' \
     -e '/Buckets: [0-9]\+/d' \
     -e '/Index Searches: [0-9]+/d' \
     -e '/Storage: Memory  Maximum Storage: [0-9]+kB/d' \
     -e '/Window: /d' \
     -e '/Batches: [0-9]+/d' \
     -e '/found [0-9]+ removable, [0-9]+ nonremovable row versions in [0-9]+ pages/d' | \
grep -av 'DEBUG:  rehashing catalog cache id' | \
grep -av 'DEBUG:  compacted fsync request queue from' | \
grep -av 'DEBUG:  creating and filling new WAL file' | \
grep -av 'DEBUG:  done creating and filling new WAL file' | \
grep -av 'DEBUG:  flushed relation because a checkpoint occurred concurrently' | \
grep -av 'NOTICE:  cancelling the background worker for job' | \
if [ "${RUNNER}" = "shared" ]; then \
    sed -e 's!_[0-9]\{1,\}_[0-9]\{1,\}_chunk!_X_X_chunk!g'; \
else \
    cat; \
fi | \
if [ "${RUNNER}" = "isolation" ]; then \
    sed -e 's!_[0-9]\{1,\}_[0-9]\{1,\}_chunk!_X_X_chunk!g' \
        -e 's!hypertable_[0-9]\{1,\}!hypertable_X!g' \
        -e 's!constraint_[0-9]\{1,\}!constraint_X!g' \
        -e 's!with OID [0-9]\{1,\}!with OID X!g'; \
else \
    cat; \
fi
