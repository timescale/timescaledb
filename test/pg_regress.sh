#!/bin/bash

# Wrapper around pg_regress to be able to set the tests to run via the
# TESTS environment variable
EXE_DIR=$(dirname $0)
PG_REGRESS=${PG_REGRESS:-pg_regress}
TESTS=${TESTS:-}

if [[ -z ${TESTS} ]]; then
    for t in ${EXE_DIR}/sql/*.sql; do
        t=${t##${EXE_DIR}/sql/}
        t=${t%.sql}
        TESTS="${TESTS} $t"
    done
fi

${PG_REGRESS} $@ ${PG_REGRESS_OPTS} ${TESTS}
