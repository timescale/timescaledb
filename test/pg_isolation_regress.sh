#!/usr/bin/env bash

# Wrapper around pg_regress to be able to override the tests to run via the
# TESTS environment variable

# NB this script mirrors the adjacent pg_regress.sh, and they should
# be kept in sync

CURRENT_DIR=$(dirname $0)
EXE_DIR=${EXE_DIR:-${CURRENT_DIR}}
SPECS_DIR=${SPECS_DIR:-${EXE_DIR}/isolation/specs}
PG_ISOLATION_REGRESS=${PG_ISOLATION_REGRESS:-pg_isolation_regress}
ISOLATION_TEST_SCHEDULE=${ISOLATION_TEST_SCHEDULE:-}
TEMP_SCHEDULE=${CURRENT_DIR}/temp_schedule
TESTS=${TESTS:-}
IGNORES=${IGNORES:-}
SKIPS=${SKIPS:-}

contains() {
    # a list contains a value foo if the regex ".* foo .*" holds true
    [[ $1 =~ (.*[[:space:]]|^)$2([[:space:]].*|$) ]];
    return $?
}

echo "TESTS ${TESTS}"
echo "IGNORES ${IGNORES}"
echo "SKIPS ${SKIPS}"

if [[ -z ${TESTS} ]]; then
    if [[ -z ${ISOLATION_TEST_SCHEDULE} ]]; then
        for t in ${SPECS_DIR}/*.spec; do
            t=${t##${SPECS_DIR}/}
            t=${t%.spec}

            if ! contains "${SKIPS}" "${t}"; then
                TESTS="${TESTS} ${t}"
            fi
        done
    elif [[ -n ${IGNORES} ]] || [[ -n ${SKIPS} ]]; then
        # get the tests from the test schedule, but ignore our IGNORES
        while read t; do
            if [[ t =~ ignore:* ]]; then
                t=${t##ignore:* }
                IGNORES="${t} ${IGNORES}"
                # run the test but ignore the result
                TESTS="${TESTS} ${t}"
                continue
            fi
            t=${t##test: }
            ## check each individual test in test group to see if it should be ignored
            ## note that now isolation tests are not grouped together and so the for loop
            ## will always run once.
            for el in ${t[@]}; do
                if ! contains "${SKIPS}" "${el}"; then
                    TESTS="${TESTS} ${el}"
                fi
            done
        done < ${ISOLATION_TEST_SCHEDULE}
        # no longer needed as the contents has been parsed above
        # and unsetting it helps take a shortcut later
        ISOLATION_TEST_SCHEDULE=
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
    for t in ${SPECS_DIR}/*.spec*; do
        t=${t##${SPECS_DIR}/}
        t=${t%.spec}
        t=${t%.spec.in}

        if contains "${FILTER}" "${t}" && ! contains "${SKIPS}" "${t}"; then
            TESTS="${TESTS} $t"
        fi
    done
    # When TESTS is specified the passed tests scheduler is not used and emptying it helps take a shortcut later
    ISOLATION_TEST_SCHEDULE=
fi

if [[ -z ${TESTS} ]] && [[ -z ${ISOLATION_TEST_SCHEDULE} ]]; then
    exit 0;
fi

touch ${TEMP_SCHEDULE}
rm ${TEMP_SCHEDULE}
touch ${TEMP_SCHEDULE}

for t in ${IGNORES}; do
    echo "ignore: ${t}" >> ${TEMP_SCHEDULE}
done

for t in ${TESTS}; do
    echo "test: ${t}" >> ${TEMP_SCHEDULE}
done

PG_ISOLATION_REGRESS_OPTS="${PG_ISOLATION_REGRESS_OPTS}  --schedule=${TEMP_SCHEDULE}"

export PGISOLATIONTIMEOUT=100

${PG_ISOLATION_REGRESS} $@ ${PG_ISOLATION_REGRESS_OPTS}
