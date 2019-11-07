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

# PGAPPNAME will be 'pg_regress/test' so we cut off the prefix
# to get the name of the test (PG 10 and 11 only)
TEST_BASE_NAME=${PGAPPNAME##pg_regress/}

# if this is a versioned test our name will have version as suffix
# so we cut off suffix to get base name
if [[ ${TEST_BASE_NAME} == *-1[0-9] ]]; then
  TEST_BASE_NAME=${TEST_BASE_NAME%???}
fi

#docker doesn't set user
USER=${USER:-`whoami`}

TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER:-super_user}
TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER:-default_perm_user}
TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2:-default_perm_user_2}

shift

# setup clusterwide settings on first run
if [[ ! -f ${TEST_OUTPUT_DIR}/.pg_init ]]; then
  touch ${TEST_OUTPUT_DIR}/.pg_init
  ${PSQL} $@ -U ${USER} -d postgres -v ECHO=none -c "ALTER USER ${TEST_ROLE_SUPERUSER} WITH SUPERUSER;" >/dev/null
  ${PSQL} $@ -U $TEST_PGUSER -d ${TEST_DBNAME} -v ECHO=none < ${TEST_INPUT_DIR}/shared/sql/include/shared_setup.sql >/dev/null
fi

cd ${EXE_DIR}/sql

# we strip out any output between <exclude_from_test></exclude_from_test>
# and the part about memory usage in EXPLAIN ANALYZE output of Sort nodes
${PSQL} -U ${TEST_PGUSER} \
     -v ON_ERROR_STOP=1 \
     -v VERBOSITY=terse \
     -v ECHO=all \
     -v TEST_BASE_NAME=${TEST_BASE_NAME} \
     -v TEST_INPUT_DIR=${TEST_INPUT_DIR} \
     -v TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR} \
     -v ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER} \
     -v ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER} \
     -v ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2} \
     $@ -d ${TEST_DBNAME} 2>&1 | sed -e '/<exclude_from_test>/,/<\/exclude_from_test>/d' -e 's! Memory: [0-9]\{1,\}kB!!' -e 's! Memory Usage: [0-9]\{1,\}kB!!'
