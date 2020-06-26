#!/usr/bin/env bash

# Wrapper around pg_isolation_regress to be able to control the schedule with environment variables
#
# The following control variables are supported:
#
# TESTS     only run tests from this list
# IGNORES   failure of tests in this list will not lead to test failure
# SKIPS     tests from this list are not run

# This script and pg_regress.sh use the same environment variables
# to decide which tests to run and should be kept in sync.

CURRENT_DIR=$(dirname $0)
EXE_DIR=${EXE_DIR:-${CURRENT_DIR}}
PG_ISOLATION_REGRESS=${PG_ISOLATION_REGRESS:-pg_isolation_regress}
TEST_SCHEDULE=${ISOLATION_TEST_SCHEDULE:-}
PG_REGRESS_DIFF_OPTS=-u
TEMP_SCHEDULE=${CURRENT_DIR}/temp_schedule
SCHEDULE=
TESTS=${TESTS:-}
IGNORES=${IGNORES:-}
SKIPS=${SKIPS:-}

contains() {
    # a list contains a value foo if the regex ".* foo .*" holds true
    [[ $1 =~ (.*[[:space:]]|^)$2([[:space:]].*|$) ]];
    return $?
}

if [[ -z ${TEST_SCHEDULE} ]];  then
  echo "No test schedule supplied please set ISOLATION_TEST_SCHEDULE"
  exit 1;
fi

echo "TESTS ${TESTS}"
echo "IGNORES ${IGNORES}"
echo "SKIPS ${SKIPS}"

if [[ -z ${TESTS} ]] && [[ -z ${SKIPS} ]] && [[ -z ${IGNORES} ]]; then
  # no filter variables set
  # nothing to do here and we can use the cmake generated schedule

  SCHEDULE=${TEST_SCHEDULE}

elif [[ -z ${TESTS} ]] && [[ -z ${SKIPS} ]] && [[ -n ${IGNORES} ]]; then
  # if we only have IGNORES we can use the cmake created schedule
  # and just prepend ignore lines for the tests whose result should be
  # ignored. This will allow us to retain the parallel groupings from
  # the supplied schedule

  echo > ${TEMP_SCHEDULE}

  for t in ${IGNORES}; do
      echo "ignore: ${t}" >> ${TEMP_SCHEDULE}
  done
  cat ${TEST_SCHEDULE} >> ${TEMP_SCHEDULE}

  SCHEDULE=${TEMP_SCHEDULE}

else
  # either TESTS or SKIPS was specified so we need to create a new schedule
  # based on those

  if [[ -z ${TESTS} ]]; then

    # get the tests from the test schedule, but ignore our IGNORES
    while read t; do
      if [[ t =~ ignore:* ]]; then
        t=${t##ignore:* }
        IGNORES="${t} ${IGNORES}"
        continue
      fi
      t=${t##test: }
      ## check each individual test in test group to see if it should be ignored
      for el in ${t[@]}; do
        if ! contains "${SKIPS}" "${el}"; then
          TESTS="${TESTS} ${el}"
        fi
      done
    done < ${TEST_SCHEDULE}
  fi

  echo > ${TEMP_SCHEDULE}

  for t in ${IGNORES}; do
      echo "ignore: ${t}" >> ${TEMP_SCHEDULE}
  done

  for t in ${TESTS}; do
      echo "test: ${t}" >> ${TEMP_SCHEDULE}
  done

  SCHEDULE=${TEMP_SCHEDULE}

fi

export PG_REGRESS_DIFF_OPTS

PG_ISOLATION_REGRESS_OPTS="${PG_ISOLATION_REGRESS_OPTS}  --schedule=${SCHEDULE}"
${PG_ISOLATION_REGRESS} $@ ${PG_ISOLATION_REGRESS_OPTS}
