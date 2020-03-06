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
if [[ ${PGAPPNAME} = pg_regress/* ]]; then
  CURRENT_TEST=${PGAPPNAME##pg_regress/}
else
  # PG 9.6 pg_regress does not pass in testname
  # so we generate unique name from pid
  CURRENT_TEST="test_$$"
fi
TEST_DBNAME="db_${CURRENT_TEST}"

# Read the extension version from version.config
read -r VERSION < ${CURRENT_DIR}/../version.config
EXT_VERSION=${VERSION##version = }

#docker doesn't set user
USER=${USER:-`whoami`}

TEST_SPINWAIT_ITERS=${TEST_SPINWAIT_ITERS:-100}

TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER:-super_user}
TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER:-default_perm_user}
TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2:-default_perm_user_2}

shift

function cleanup {
  ${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d postgres -v ECHO=none -c "DROP DATABASE \"${TEST_DBNAME}\";" >/dev/null
}

trap cleanup EXIT

# setup clusterwide settings on first run
if [[ ! -f ${TEST_OUTPUT_DIR}/.pg_init ]]; then
  touch ${TEST_OUTPUT_DIR}/.pg_init
  ${PSQL} $@ -U ${USER} -d postgres -v ECHO=none -c "ALTER USER ${TEST_ROLE_SUPERUSER} WITH SUPERUSER;" >/dev/null
fi

cd ${EXE_DIR}/sql

# create database and install timescaledb
${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d postgres -v ECHO=none -c "CREATE DATABASE \"${TEST_DBNAME}\";"
${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d ${TEST_DBNAME} -v ECHO=none -c "SET client_min_messages=error; CREATE EXTENSION timescaledb;"
${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d ${TEST_DBNAME} -v ECHO=none -v MODULE_PATHNAME="'timescaledb-${EXT_VERSION}'" < ${CURRENT_DIR}/sql/utils/testsupport.sql >/dev/null 2>&1

export TEST_DBNAME

# we strip out any output between <exclude_from_test></exclude_from_test>
# and the part about memory usage in EXPLAIN ANALYZE output of Sort nodes
${PSQL} -U ${TEST_PGUSER} \
     -v ON_ERROR_STOP=1 \
     -v VERBOSITY=terse \
     -v ECHO=all \
     -v DISABLE_OPTIMIZATIONS=off \
     -v TEST_DBNAME="${TEST_DBNAME}" \
     -v TEST_TABLESPACE1_PATH=\'${TEST_TABLESPACE1_PATH}\' \
     -v TEST_TABLESPACE2_PATH=\'${TEST_TABLESPACE2_PATH}\' \
     -v TEST_INPUT_DIR=${TEST_INPUT_DIR} \
     -v TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR} \
     -v TEST_SPINWAIT_ITERS=${TEST_SPINWAIT_ITERS} \
     -v ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER} \
     -v ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER} \
     -v ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2} \
     -v MODULE_PATHNAME="'timescaledb-${EXT_VERSION}'" \
     -v TSL_MODULE_PATHNAME="'timescaledb-tsl-${EXT_VERSION}'" \
     $@ -d ${TEST_DBNAME} 2>&1 | sed -e '/<exclude_from_test>/,/<\/exclude_from_test>/d' -e 's! Memory: [0-9]\{1,\}kB!!' -e 's! Memory Usage: [0-9]\{1,\}kB!!'
