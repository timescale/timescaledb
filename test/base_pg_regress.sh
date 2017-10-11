#!/bin/bash
#
# This script sets up temporary test file directories to run
# pg_regress using the pg standard tests with timescaledb
# loaded. Some tests will have different output and when they
# should, the changed output files updated output files should
# be put in the SQL_BASE_DIFF_FILES_DIR directory.
#
: "${PG_REGRESS_DIR?Need to set PG_REGRESS_DIR}"
set -x
PG_REGRESS=${PG_REGRESS:-pg_regress}
TESTS=${TESTS:-}

SCRIPT_DIR=$(dirname $0)

SQL_BASE_TEST_WORKING_DIR=${SQL_BASE_TEST_WORKING_DIR:-${SCRIPT_DIR}/base_test}

SQL_BASE_SQL_FILES_DIR=${SQL_BASE_SQL_FILES_DIR:-${PG_REGRESS_DIR}/sql}
SQL_BASE_EXPECTED_FILES_DIR=${SQL_BASE_EXPECTED_FILES_DIR:-${PG_REGRESS_DIR}/expected}
SQL_BASE_SOURCE_FILES_DIR=${SQL_BASE_SOURCE_FILES_DIR:-${PG_REGRESS_DIR}/input}

SQL_BASE_TMP_EXPECTED_FILES_DIR=${SQL_BASE_TMP_EXPECTED_FILES_DIR:-${SQL_BASE_TEST_WORKING_DIR}/expected}
SQL_BASE_TMP_SOURCE_FILES_DIR=${SQL_BASE_TMP_SOURCE_FILES_DIR:-${SQL_BASE_TEST_WORKING_DIR}/input}
SQL_BASE_TMP_SQL_FILES_DIR=${SQL_BASE_TMP_SQL_FILES_DIR:-${SQL_BASE_TEST_WORKING_DIR}/sql}

SQL_BASE_DIFF_FILES_DIR=${SQL_BASE_DIFF_FILES_DIR:-${SCRIPT_DIR}/base_test_diff_files}

mkdir -p ${SQL_BASE_TMP_EXPECTED_FILES_DIR}
mkdir -p ${SQL_BASE_TMP_SOURCE_FILES_DIR}
mkdir -p ${SQL_BASE_TMP_SQL_FILES_DIR}

cp ${SQL_BASE_SOURCE_FILES_DIR}/*.source ${SQL_BASE_TMP_SOURCE_FILES_DIR}/
cp ${SQL_BASE_SQL_FILES_DIR}/*.sql ${SQL_BASE_TMP_SQL_FILES_DIR}/

if [[ -z ${TESTS} ]]; then
    TESTS=`cat ${PG_REGRESS_DIR}/serial_schedule | grep '^test:'  | sed 's/test: //g' | grep -v -F -f base_test_excludes.txt | tr '\n' ' '`
fi

for f in ${TESTS}
do
    TEST_FILE=${f}.sql
    EXPECTED_FILE=${f}.out

    if [ -e ${SQL_BASE_DIFF_FILES_DIR}/${EXPECTED_FILE} ]
    then
        cp ${SQL_BASE_DIFF_FILES_DIR}/${EXPECTED_FILE} ${SQL_BASE_TMP_EXPECTED_FILES_DIR}/
    else
        cp ${SQL_BASE_EXPECTED_FILES_DIR}/${EXPECTED_FILE} ${SQL_BASE_TMP_EXPECTED_FILES_DIR}/
    fi
done

${PG_REGRESS} $@ ${PG_REGRESS_OPTS} ${TESTS}
