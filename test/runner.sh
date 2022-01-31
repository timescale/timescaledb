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

# PGAPPNAME will be 'pg_regress/test' so we cut off the prefix
# to get the name of the test
CURRENT_TEST=${PGAPPNAME##pg_regress/}

# Since different PG version tests cannot run in parallel in the same instance,
# we remove the trailing version suffix to get a good symbol that can be
# used as identifier as well.
TEST_DBNAME="db_${CURRENT_TEST%%-[0-9][0-9]}"

# Read the extension version from version.config
read -r VERSION < ${CURRENT_DIR}/../version.config
EXT_VERSION=${VERSION##version = }

#docker doesn't set user
USER=${USER:-$(whoami)}

TEST_SPINWAIT_ITERS=${TEST_SPINWAIT_ITERS:-1000}

TEST_ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER:-super_user}
TEST_ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER:-default_perm_user}
TEST_ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2:-default_perm_user_2}

# Users for clustering. These users have password auth enabled in pg_hba.conf
TEST_ROLE_CLUSTER_SUPERUSER=${TEST_ROLE_CLUSTER_SUPERUSER:-cluster_superuser}
TEST_ROLE_1=${TEST_ROLE_1:-test_role_1}
TEST_ROLE_2=${TEST_ROLE_2:-test_role_2}
TEST_ROLE_2_PASS=${TEST_ROLE_2_PASS:-pass}
TEST_ROLE_3=${TEST_ROLE_3:-test_role_3}
TEST_ROLE_3_PASS=${TEST_ROLE_3_PASS:-pass}
TEST_ROLE_READ_ONLY=${TEST_ROLE_READ_ONLY:-test_role_read_only}

shift

# Drop test database and make it less verbose in case of dropping a
# distributed database.
function cleanup {
  cat <<EOF | ${PSQL} "$@" -U $TEST_ROLE_SUPERUSER -d postgres -v ECHO=none >/dev/null 2>&1
    SET client_min_messages=ERROR;
    DROP DATABASE "${TEST_DBNAME}";
EOF
}

trap cleanup EXIT

# setup clusterwide settings on first run
# we use mkdir here because it is an atomic operation unlike existance of a lockfile
# where creating and checking are 2 separate operations
if mkdir ${TEST_OUTPUT_DIR}/.pg_init 2>/dev/null; then
  ${PSQL} "$@" -U ${USER} -d template1 -v ECHO=none >/dev/null 2>&1 <<EOF
    SET client_min_messages=ERROR;

    DO \$\$
      BEGIN
        IF current_setting('server_version_num')::int >= 150000 THEN
          GRANT CREATE ON SCHEMA public TO ${TEST_PGUSER};
          GRANT CREATE ON SCHEMA public TO ${TEST_ROLE_DEFAULT_PERM_USER};
          GRANT CREATE ON SCHEMA public TO ${TEST_ROLE_DEFAULT_PERM_USER_2};
          GRANT CREATE ON SCHEMA public TO ${TEST_ROLE_1};
          GRANT CREATE ON SCHEMA public TO ${TEST_ROLE_2};
          GRANT CREATE ON SCHEMA public TO ${TEST_ROLE_3};
        END IF;
      END
    \$\$ LANGUAGE PLPGSQL;

    ALTER USER ${TEST_ROLE_SUPERUSER} WITH SUPERUSER;
    ALTER USER ${TEST_ROLE_CLUSTER_SUPERUSER} WITH SUPERUSER;
    ALTER USER ${TEST_ROLE_1} WITH CREATEDB CREATEROLE;
    ALTER USER ${TEST_ROLE_2} WITH CREATEDB PASSWORD '${TEST_ROLE_2_PASS}';
    ALTER USER ${TEST_ROLE_3} WITH CREATEDB PASSWORD '${TEST_ROLE_3_PASS}';
EOF
  ${PSQL} "$@" -U ${USER} -d postgres -v ECHO=none -c "ALTER USER ${TEST_ROLE_SUPERUSER} WITH SUPERUSER;" >/dev/null
  touch ${TEST_OUTPUT_DIR}/.pg_init/done
fi

# we need to wait for cluster setup to finish cause with parallel schedule
# multiple instances will be running and mkdir will only succeed on the first runner
while [ ! -f ${TEST_OUTPUT_DIR}/.pg_init/done ]; do sleep 0.2; done

cd ${EXE_DIR}/sql

# create database and install timescaledb
${PSQL} "$@" -U $TEST_ROLE_SUPERUSER -d postgres -v ECHO=none -c "CREATE DATABASE \"${TEST_DBNAME}\";"
${PSQL} "$@" -U $TEST_ROLE_SUPERUSER -d ${TEST_DBNAME} -v ECHO=none -c "SET client_min_messages=error; CREATE EXTENSION timescaledb; GRANT USAGE ON FOREIGN DATA WRAPPER timescaledb_fdw TO ${TEST_ROLE_1};"
${PSQL} "$@" -U $TEST_ROLE_SUPERUSER -d ${TEST_DBNAME} -v ECHO=none -v MODULE_PATHNAME="'timescaledb-${EXT_VERSION}'" -v TSL_MODULE_PATHNAME="'timescaledb-tsl-${EXT_VERSION}'" < ${TEST_SUPPORT_FILE} >/dev/null 2>&1
export TEST_DBNAME

# we strip out any output between <exclude_from_test></exclude_from_test>
# and the part about memory usage in EXPLAIN ANALYZE output of Sort nodes
${PSQL} -U ${TEST_PGUSER} \
     -v ON_ERROR_STOP=1 \
     -v VERBOSITY=terse \
     -v ECHO=all \
     -v TEST_DBNAME="${TEST_DBNAME}" \
     -v TEST_TABLESPACE1_PREFIX=${TEST_TABLESPACE1_PREFIX} \
     -v TEST_TABLESPACE2_PREFIX=${TEST_TABLESPACE2_PREFIX} \
     -v TEST_TABLESPACE3_PREFIX=${TEST_TABLESPACE3_PREFIX} \
     -v TEST_TABLESPACE1_PATH=\'${TEST_TABLESPACE1_PATH}\' \
     -v TEST_TABLESPACE2_PATH=\'${TEST_TABLESPACE2_PATH}\' \
     -v TEST_TABLESPACE3_PATH=\'${TEST_TABLESPACE3_PATH}\' \
     -v TEST_INPUT_DIR=${TEST_INPUT_DIR} \
     -v TEST_OUTPUT_DIR=${TEST_OUTPUT_DIR} \
     -v TEST_SPINWAIT_ITERS=${TEST_SPINWAIT_ITERS} \
     -v ROLE_SUPERUSER=${TEST_ROLE_SUPERUSER} \
     -v ROLE_DEFAULT_PERM_USER=${TEST_ROLE_DEFAULT_PERM_USER} \
     -v ROLE_DEFAULT_PERM_USER_2=${TEST_ROLE_DEFAULT_PERM_USER_2} \
     -v ROLE_CLUSTER_SUPERUSER=${TEST_ROLE_CLUSTER_SUPERUSER} \
     -v ROLE_1=${TEST_ROLE_1} \
     -v ROLE_2=${TEST_ROLE_2} \
     -v ROLE_3=${TEST_ROLE_3} \
     -v ROLE_READ_ONLY=${TEST_ROLE_READ_ONLY} \
     -v ROLE_2_PASS=${TEST_ROLE_2_PASS} \
     -v ROLE_3_PASS=${TEST_ROLE_3_PASS} \
     -v MODULE_PATHNAME="'timescaledb-${EXT_VERSION}'" \
     -v TSL_MODULE_PATHNAME="'timescaledb-tsl-${EXT_VERSION}'" \
     -v TEST_SUPPORT_FILE=${TEST_SUPPORT_FILE} \
     "$@" -d ${TEST_DBNAME} 2>&1 | sed -e '/<exclude_from_test>/,/<\/exclude_from_test>/d' -e 's! Memory: [0-9]\{1,\}kB!!' -e 's! Memory Usage: [0-9]\{1,\}kB!!' -e 's! Average  Peak Memory: [0-9]\{1,\}kB!!'
