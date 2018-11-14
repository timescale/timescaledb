#!/usr/bin/env bash

set -u
set -e
EXE_DIR=$(dirname $0)
PG_REGRESS_PSQL=$1
PSQL=${PSQL:-$PG_REGRESS_PSQL}
TEST_PGUSER=${TEST_PGUSER:-postgres}
TEST_DBNAME=${TEST_DBNAME:-single}
TEST_DBNAME2=${TEST_DBNAME2:-${TEST_DBNAME}_2}
TEST_INPUT_DIR=${TEST_INPUT_DIR:-${EXE_DIR}}
TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR:-${EXE_DIR}}

# Read the extension version from version.config
read -r VERSION < ../version.config
EXT_VERSION=${VERSION##version = }

#docker doesn't set user
USER=${USER:-`whoami`}
# This mktemp line will work on both OSX and GNU systems
TEST_TABLESPACE1_PATH=${TEST_TABLESPACE1_PATH:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_regress')}
TEST_TABLESPACE2_PATH=${TEST_TABLESPACE2_PATH:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_regress')}

TEST_SPINWAIT_ITERS=${TEST_SPINWAIT_ITERS:-10}

TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER:-super_user}
TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER:-default_perm_user}
TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2:-default_perm_user_2}

shift

# Keep original args around to use in cleanup
ORIG_ARGS=("$@")

function cleanup {
    rm -rf ${TEST_TABLESPACE1_PATH}
    rm -rf ${TEST_TABLESPACE2_PATH}
    # When testing remotely, the secondary database does not get removed, so
    # remove it directly because it causes problems when trying to drop super
    # user roles on next run (i.e., the db is a dependency of the role).
    # Problem is not noticed when using `make installcheck` because it makes a
    # new instance each time.
    ${PSQL} ${ORIG_ARGS} -U ${USER} -d postgres -v ECHO=none -c "DROP DATABASE IF EXISTS ${TEST_DBNAME2};" > /dev/null 2>&1 || :
}

trap cleanup EXIT

# Setup directories required by tests
cd ${EXE_DIR}/sql
mkdir -p ${TEST_TABLESPACE1_PATH}
mkdir -p ${TEST_TABLESPACE2_PATH}
mkdir -p dump

# set role permissions and reset database
${PSQL} $@ -U $USER -d postgres -v ECHO=none -c "ALTER USER $TEST_ROLE_SUPERUSER WITH SUPERUSER;"
${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d postgres -v ECHO=none -c "DROP DATABASE $TEST_DBNAME;" >/dev/null 2>&1 || :
${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d postgres -v ECHO=none -c "DROP DATABASE $TEST_DBNAME2;" >/dev/null 2>&1 || :
${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d postgres -v ECHO=none -c "CREATE DATABASE $TEST_DBNAME;"
${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d postgres -v ECHO=none -c "CREATE DATABASE $TEST_DBNAME2;"
${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d single -v ECHO=none -c "set client_min_messages=error; CREATE EXTENSION timescaledb;"
${PSQL} $@ -U $TEST_ROLE_SUPERUSER -d single -v ECHO=none -v MODULE_PATHNAME="'timescaledb-${EXT_VERSION}'" < ${EXE_DIR}/sql/utils/testsupport.sql >/dev/null 2>&1 || :

${PSQL} -U ${TEST_PGUSER} \
     -v ON_ERROR_STOP=1 \
     -v VERBOSITY=terse \
     -v ECHO=all \
     -v DISABLE_OPTIMIZATIONS=off \
     -v TEST_TABLESPACE1_PATH=\'${TEST_TABLESPACE1_PATH}\' \
     -v TEST_TABLESPACE2_PATH=\'${TEST_TABLESPACE2_PATH}\' \
     -v TEST_INPUT_DIR=${TEST_INPUT_DIR} \
     -v TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR} \
     -v TEST_SPINWAIT_ITERS=${TEST_SPINWAIT_ITERS} \
     -v ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER} \
     -v ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER} \
     -v ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2} \
     -v MODULE_PATHNAME="'timescaledb-${EXT_VERSION}'" \
     $@ -d single 2>&1 | sed '/<exclude_from_test>/,/<\/exclude_from_test>/d'
