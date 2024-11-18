#!/usr/bin/env bash

set -u
set -e
CURRENT_DIR=$(dirname $0)
EXE_DIR=${EXE_DIR:-${CURRENT_DIR}}
PG_REGRESS_PSQL=$1
PSQL=${PSQL:-$PG_REGRESS_PSQL}
PSQL="${PSQL} -X" # Prevent any .psqlrc files from being executed during the tests
TEST_PGUSER=${TEST_PGUSER:-postgres}
TEST_INPUT_DIR=${TEST_INPUT_DIR:-${EXE_DIR}}
TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR:-${EXE_DIR}}
TEST_SUPPORT_FILE=${CURRENT_DIR}/sql/utils/testsupport.sql
TEST_SUPPORT_FILE_INIT=${CURRENT_DIR}/sql/utils/testsupport_init.sql

# Read the extension version from version.config
read -r VERSION < ${CURRENT_DIR}/../version.config
EXT_VERSION=${VERSION##version = }

# PGAPPNAME will be 'pg_regress/test' so we cut off the prefix
# to get the name of the test
TEST_BASE_NAME=${PGAPPNAME##pg_regress/}

# if this is a versioned test our name will have version as suffix
# so we cut off suffix to get base name
if [[ ${TEST_BASE_NAME} == *-1[0-9] ]]; then
  TEST_BASE_NAME=${TEST_BASE_NAME%???}
fi

#docker doesn't set user
USER=${USER:-$(whoami)}

TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER:-super_user}
TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER:-default_perm_user}
TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2:-default_perm_user_2}

shift

# setup clusterwide settings on first run
# we use mkdir here because it is an atomic operation unlike existence of a lockfile
# where creating and checking are 2 separate operations
if mkdir ${TEST_OUTPUT_DIR}/.pg_init 2>/dev/null; then
  ${PSQL} "$@" -U ${USER} -d postgres -v ECHO=none -c "ALTER USER ${TEST_ROLE_SUPERUSER} WITH SUPERUSER;" >/dev/null
  ${PSQL} -U ${USER} \
     -v MODULE_PATHNAME="'timescaledb-${EXT_VERSION}'" \
     -v ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER} \
     -v ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2} \
     -v ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER} \
     -v TEST_BASE_NAME=${TEST_BASE_NAME} \
     -v TEST_DBNAME="${TEST_DBNAME}" \
     -v TEST_INPUT_DIR=${TEST_INPUT_DIR} \
     -v TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR} \
     -v TEST_SUPPORT_FILE=${TEST_SUPPORT_FILE} \
     -v TEST_SUPPORT_FILE_INIT=${TEST_SUPPORT_FILE_INIT} \
     -v TSL_MODULE_PATHNAME="'timescaledb-tsl-${EXT_VERSION}'" \
     "$@" -d ${TEST_DBNAME} < ${TEST_INPUT_DIR}/shared/sql/include/shared_setup.sql >/dev/null
  touch ${TEST_OUTPUT_DIR}/.pg_init/done
fi

# we need to wait for cluster setup to finish cause with parallel schedule
# multiple instances will be running and mkdir will only succeed on the first runner
while [ ! -f ${TEST_OUTPUT_DIR}/.pg_init/done ]; do sleep 0.2; done

cd ${EXE_DIR}/sql

# we strip out any output between <exclude_from_test></exclude_from_test>
# and the part about memory usage in EXPLAIN ANALYZE output of Sort nodes
# also ignore the Postgres rehashing catalog debug messages from 'src/backend/utils/cache/catcache.c'
${PSQL} -U ${TEST_PGUSER} \
     -v ON_ERROR_STOP=1 \
     -v VERBOSITY=terse \
     -v ECHO=all \
     -v TEST_DBNAME="${TEST_DBNAME}" \
     -v TEST_BASE_NAME=${TEST_BASE_NAME} \
     -v TEST_INPUT_DIR=${TEST_INPUT_DIR} \
     -v TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR} \
     -v ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER} \
     -v ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER} \
     -v ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2} \
     -v MODULE_PATHNAME="'timescaledb-${EXT_VERSION}'" \
     -v TSL_MODULE_PATHNAME="'timescaledb-tsl-${EXT_VERSION}'" \
     "$@" -d ${TEST_DBNAME} 2>&1 | \
          sed  -e '/<exclude_from_test>/,/<\/exclude_from_test>/d' \
               -e 's!_[0-9]\{1,\}_[0-9]\{1,\}_chunk!_X_X_chunk!g' \
               -e 's!^ \{1,\}QUERY PLAN \{1,\}$!QUERY PLAN!' \
               -e 's!:  actual rows!: actual rows!' \
               -e '/^-\{1,\}$/d' \
               -e 's! Memory: [0-9]\{1,\}kB!!' \
               -e 's! Memory Usage: [0-9]\{1,\}kB!!' \
               -e 's! Average  Peak Memory: [0-9]\{1,\}kB!!' | \
          grep -v 'DEBUG:  rehashing catalog cache id' | \
          grep -v 'DEBUG:  compacted fsync request queue from' | \
          grep -v 'DEBUG:  creating and filling new WAL file' | \
          grep -v 'DEBUG:  done creating and filling new WAL file' | \
          grep -v 'DEBUG:  flushed relation because a checkpoint occurred concurrently' | \
          grep -v 'NOTICE:  cancelling the background worker for job'
