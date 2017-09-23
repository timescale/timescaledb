#!/bin/bash

# Wrapper around pg_regress to be able to override the tests to run via the
# TESTS environment variable
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
fi

${PG_REGRESS} $@ ${PG_REGRESS_OPTS} ${TESTS}
