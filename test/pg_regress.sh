#!/usr/bin/env bash

# Wrapper around pg_regress to be able to override the tests to run via the
# TESTS environment variable

# NB this script mirrors the adjacent pg_isolation_regress.sh, and they should
# be kept in sync

CURRENT_DIR=$(dirname $0)
EXE_DIR=${EXE_DIR:-${CURRENT_DIR}}
PG_REGRESS=${PG_REGRESS:-pg_regress}
TEST_SCHEDULE=${TEST_SCHEDULE:-}
TESTS=${TESTS:-}
IGNORES=${IGNORES:-}

contains() {
    # a list contains a value foo if the regex ".* foo .*" holds true
    [[ $1 =~ (.*[[:space:]]|^)$2([[:space:]].*|$) ]];
    return $?
}

if [[ -z ${TESTS} ]]; then
    if [[ -z ${TEST_SCHEDULE} ]]; then
        for t in ${EXE_DIR}/sql/*.sql; do
            t=${t##${EXE_DIR}/sql/}
            t=${t%.sql}

            if ! contains "${IGNORES}" "${t}"; then
                TESTS="${TESTS} ${t}"
            fi
        done
    elif [[ -n ${IGNORES} ]]; then
        # get the tests from the test schedule, but ignore our IGNORES
        while read t; do
            if [[ t =~ ignore:* ]]; then
                t=${t##ignore:* }
                IGNORES="${t} ${IGNORES}"
                continue
            fi
            t=${t##test: }
            if ! contains "${IGNORES}" "${t}"; then
                TESTS="${TESTS} ${t}"
            fi
        done < ${TEST_SCHEDULE}
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

        if contains "${FILTER}" "${t}" && ! contains "${IGNORES}" "${t}"; then
            TESTS="${TESTS} $t"
        fi
    done
fi

if [[ -z ${TESTS} ]] && [[ -z ${TEST_SCHEDULE} ]]; then
    exit 0;
fi

${PG_REGRESS} $@ ${PG_REGRESS_OPTS} ${TESTS}
