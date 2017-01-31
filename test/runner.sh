#!/usr/bin/env bash

set -u
set -e

PG_REGRESS_PSQL=$1
shift
PSQL=${PSQL:-$PG_REGRESS_PSQL}
TEST_TABLESPACE_PATH=${TEST_TABLESPACE_PATH:-/tmp/tspace1}

cd test/sql
PG_PROC_USER=$(ps u | awk '/postgres/ { print $1; exit }')
mkdir -p ${TEST_TABLESPACE_PATH}
mkdir -p dump

exec ${PSQL} -v ON_ERROR_STOP=1 -v TEST_TABLESPACE_PATH=\'${TEST_TABLESPACE_PATH}\' $@
