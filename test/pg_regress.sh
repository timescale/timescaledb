#!/usr/bin/env bash

# Wrapper around pg_regress to be able to override the tests to run via the
# TESTS environment variable

# NB this script mirrors the adjacent pg_isolation_regress.sh, and they should
# be kept in sync

EXE_DIR=$(dirname $0)
PG_REGRESS=${PG_REGRESS:-pg_regress}
TEST_SCHEDULE=${TEST_SCHEDULE:-}
TESTS=${TESTS:-}

if [[ -z ${TESTS} ]]; then
    if [[ -z ${TEST_SCHEDULE} ]]; then
        for t in ${EXE_DIR}/sql/*.sql; do
            t=${t##${EXE_DIR}/sql/}
            t=${t%.sql}
            TESTS="${TESTS} $t"
        done
    else
        PG_REGRESS_OPTS="${PG_REGRESS_OPTS} --schedule=${TEST_SCHEDULE}"
    fi
else
    # Both this and pg_isolation_regress.sh use the same TESTS env var to decide which tests to run.
    # Since we only want to pass the test runner the kind of tests it can understand,
    # and only those which actually exist, we use TESTS as a filter for the test folder,
    # passing in only those tests from the directory which are found in TESTS
    FILTER=${TESTS}
    TESTS=
    for t in ${EXE_DIR}/sql/*.sql; do
        t=${t##${EXE_DIR}/sql/}
        t=${t%.sql}
        # we use the following chain of comparisons to properly handle the case
        # where a test name is a substring of another
        if [[ $FILTER = "$t" ]]; then # one test
            TESTS="${TESTS} $t"
        elif [[ $FILTER = "$t "* ]]; then # first test in the list
            TESTS="${TESTS} $t"
        elif [[ $FILTER = *" $t" ]]; then # last test in the list
            TESTS="${TESTS} $t"
        elif [[ $FILTER = *" $t "* ]]; then # test in middle of the list
            TESTS="${TESTS} $t"
        fi
    done
fi

${PG_REGRESS} $@ ${PG_REGRESS_OPTS} ${TESTS}
