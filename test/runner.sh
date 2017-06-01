#!/usr/bin/env bash

set -u
set -e

PG_REGRESS_PSQL=$1
PSQL=${PSQL:-$PG_REGRESS_PSQL}
TEST_PGUSER=${TEST_PGUSER:-postgres}
TEST_TABLESPACE_PATH=${TEST_TABLESPACE_PATH:-/tmp/tspace1}

shift

# Setup directories required by tests
cd test/sql
mkdir -p ${TEST_TABLESPACE_PATH}
mkdir -p dump

# Hack to grant TEST_PGUSER superuser status so that we can
# consistently run tests using the same user rather than the
# current/local user
${PSQL} $@ -v ECHO=none -c "ALTER USER ${TEST_PGUSER} WITH SUPERUSER;"

exec ${PSQL} -U ${TEST_PGUSER} -v ON_ERROR_STOP=1 -v VERBOSITY=terse -v ECHO=all -v TEST_TABLESPACE_PATH=\'${TEST_TABLESPACE_PATH}\' $@
