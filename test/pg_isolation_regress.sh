#!/bin/bash

# Wrapper around pg_regress to be able to override the tests to run via the
# TESTS environment variable

# NB this script mirrors the adjacent pg_regress.sh, and they should
#    kept in synch

EXE_DIR=$(dirname $0)
PG_ISOLATION_REGRESS=${PG_ISOLATION_REGRESS:-pg_isolation_regress}
ISOLATION_TEST_SCHEDULE=${ISOLATION_TEST_SCHEDULE:-}
TESTS=${TESTS:-}

if [[ -z ${TESTS} ]]; then
    if [[ -z ${ISOLATION_TEST_SCHEDULE} ]]; then
        for t in ${EXE_DIR}/isolation/specs/*.spec; do
            t=${t##${EXE_DIR}/isolation/specs/}
            t=${t%.spec}
            TESTS="${TESTS} $t"
        done
    else
        PG_ISOLATION_REGRESS_OPTS="${PG_ISOLATION_REGRESS_OPTS} --schedule=${ISOLATION_TEST_SCHEDULE}"
    fi
else
    # Both this and pg_regress.sh use the same TESTS env var to decide which tests to run.
    # Since we only want to pass the test runner the kind of tests it can understand,
    # and only those which actually exist, we use TESTS as a filter for the test folder,
    # passing in only those tests from the directory which are found in TESTS
    FILTER=${TESTS}
    TESTS=
    for t in ${EXE_DIR}/isolation/specs/*.spec; do
        t=${t##${EXE_DIR}/isolation/specs/}
        t=${t%.spec}
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

${PG_ISOLATION_REGRESS} $@ ${PG_ISOLATION_REGRESS_OPTS} ${TESTS}
