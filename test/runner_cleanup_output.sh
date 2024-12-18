#!/usr/bin/env bash

set -u
set -e

RUNNER=${1:-""}

sed  -e '/<exclude_from_test>/,/<\/exclude_from_test>/d' \
     -e 's! Memory: [0-9]\{1,\}kB!!' \
     -e 's! Memory Usage: [0-9]\{1,\}kB!!' \
     -e 's! Average  Peak Memory: [0-9]\{1,\}kB!!' \
     -e '/Heap Fetches: [0-9]\{1,\}/d' \
     -e '/found [0-9]\{1,\} removable, [0-9]\{1,\} nonremovable row versions in [0-9]\{1,\} pages/d' | \
grep -v 'DEBUG:  rehashing catalog cache id' | \
grep -v 'DEBUG:  compacted fsync request queue from' | \
grep -v 'DEBUG:  creating and filling new WAL file' | \
grep -v 'DEBUG:  done creating and filling new WAL file' | \
grep -v 'DEBUG:  flushed relation because a checkpoint occurred concurrently' | \
grep -v 'NOTICE:  cancelling the background worker for job' | \
if [ "${RUNNER}" = "shared" ]; then \
    sed -e '/^-\{1,\}$/d' \
        -e 's!_[0-9]\{1,\}_[0-9]\{1,\}_chunk!_X_X_chunk!g' \
        -e 's!^ \{1,\}QUERY PLAN \{1,\}$!QUERY PLAN!'; \
else \
    cat; \
fi | \
if [ "${RUNNER}" = "isolation" ]; then \
    sed -e 's!_[0-9]\{1,\}_[0-9]\{1,\}_chunk!_X_X_chunk!g' \
        -e 's!hypertable_[0-9]\{1,\}!hypertable_X!g'; \
else \
    cat; \
fi
