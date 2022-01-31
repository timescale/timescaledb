#!/usr/bin/env bash

# Wrapper around pg_regress and pg_isolation_regress to be able to control the schedule with environment variables
#
# The following control variables are supported:
#
# TESTS     only run tests from this list
# IGNORES   failure of tests in this list will not lead to test failure
# SKIPS     tests from this list are not run
#
# In TESTS you may use wildcards to match multiple test names
# TESTS="compression*" will match all tests whose name starts with compression
# TESTS="*compression*" will match all tests that have compression anywhere in the name
# Wildcard matching also applies to version specific tests so compression-13
# would also be matched by those patterns.

CURRENT_DIR=$(dirname $0)
EXE_DIR=${EXE_DIR:-${CURRENT_DIR}}
PG_REGRESS=${PG_REGRESS:-pg_regress}
PG_REGRESS_DIFF_OPTS=-u
TEST_SCHEDULE=${TEST_SCHEDULE:-}
TEMP_SCHEDULE=${CURRENT_DIR}/temp_schedule
SCHEDULE=
TESTS=${TESTS:-}
IGNORES=${IGNORES:-}
SKIPS=${SKIPS:-}
PSQL=${PSQL:-psql}
PSQL="${PSQL} -X" # Prevent any .psqlrc files from being executed during the tests

# check if test matches any of the patterns in a list
# $1 list of patterns or test names
# $2 test name
# we use == intentionally and not =~ because the pattern syntax differs between
# those two and == allows for simpler patterns. With == the pattern to match
# all bgw tests would be "*bgw*" while with =~ it would be ".*bgw.*"
matches() {
  for pattern in $1; do
    # shellcheck disable=SC2053
    # We do want to match globs in $pattern here.
    if [[ $2 == $pattern ]]; then
      return 0
    fi
  done
  return 1
}

if [[ -z ${TEST_SCHEDULE} ]];  then
  echo "No test schedule supplied please set TEST_SCHEDULE"
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

  ALL_TESTS=$(grep '^test: ' ${TEST_SCHEDULE} | sed -e 's!^test: !!' |tr '\n' ' ')

  if [[ -z "${TESTS}" ]]; then
    TESTS=${ALL_TESTS}
  fi

  # build new test list in current_tests removing entries in SKIPS and
  # validating against schedule as TESTS might contain tests from
  # multiple suites and not apply to current run
  current_tests=""
  for test_pattern in ${TESTS}; do
    for test_name in ${ALL_TESTS}; do
      if ! matches "${SKIPS}" "${test_name}"; then
        # shellcheck disable=SC2053
        # We do want to match globs in $test_pattern here.
        if [[ $test_name == $test_pattern ]]; then
          current_tests="${current_tests} ${test_name}"
        fi
      fi
    done
  done

  # if none of the tests survived filtering we can exit early
  if [[ -z "${current_tests}" ]]; then
    exit 0
  fi

  current_tests=$(echo "${current_tests}" | tr ' ' '\n' | sort)

  TESTS=${current_tests}

  echo > ${TEMP_SCHEDULE}

  for t in ${IGNORES}; do
      echo "ignore: ${t}" >> ${TEMP_SCHEDULE}
  done

  for t in ${TESTS}; do
      echo "test: ${t}" >> ${TEMP_SCHEDULE}
  done

  SCHEDULE=${TEMP_SCHEDULE}

fi

function cleanup() {
  rm -rf ${EXE_DIR}/sql/dump
  rm -rf ${TEST_TABLESPACE1_PREFIX}
  rm -rf ${TEST_TABLESPACE2_PREFIX}
  rm -rf ${TEST_TABLESPACE3_PREFIX}
  rm -f ${TEMP_SCHEDULE}
  cat <<EOF | ${PSQL} -U ${USER} -h ${TEST_PGHOST} -p ${TEST_PGPORT} -d template1 >/dev/null 2>&1
    DROP TABLESPACE IF EXISTS tablespace1;
    DROP TABLESPACE IF EXISTS tablespace2;
    DROP TABLESPACE IF EXISTS tablespace3;
EOF
  rm -rf ${TEST_OUTPUT_DIR}/.pg_init
}

trap cleanup EXIT

# Generating a prefix directory for all test tablespaces. This should
# be used to build a full path for the tablespace. Note that we
# terminate the prefix with the directory separator so that we can
# easily generate paths independent of the OS.
#
# This mktemp line will work on both OSX and GNU systems
TEST_TABLESPACE1_PREFIX=${TEST_TABLESPACE1_PREFIX:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_regress')/}
TEST_TABLESPACE2_PREFIX=${TEST_TABLESPACE2_PREFIX:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_regress')/}
TEST_TABLESPACE3_PREFIX=${TEST_TABLESPACE3_PREFIX:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_regress')/}

# Creating some defaults for transitioning tests to use the prefix.
TEST_TABLESPACE1_PATH=${TEST_TABLESPACE1_PATH:-${TEST_TABLESPACE1_PREFIX}_default}
TEST_TABLESPACE2_PATH=${TEST_TABLESPACE2_PATH:-${TEST_TABLESPACE2_PREFIX}_default}
TEST_TABLESPACE3_PATH=${TEST_TABLESPACE3_PATH:-${TEST_TABLESPACE3_PREFIX}_default}
mkdir $TEST_TABLESPACE1_PATH $TEST_TABLESPACE2_PATH $TEST_TABLESPACE3_PATH

export TEST_TABLESPACE1_PREFIX TEST_TABLESPACE2_PREFIX TEST_TABLESPACE3_PREFIX
export TEST_TABLESPACE1_PATH TEST_TABLESPACE2_PATH TEST_TABLESPACE3_PATH

rm -rf ${TEST_OUTPUT_DIR}/.pg_init
mkdir -p ${EXE_DIR}/sql/dump

export PG_REGRESS_DIFF_OPTS

PG_REGRESS_OPTS="${PG_REGRESS_OPTS}  --schedule=${SCHEDULE}"
${PG_REGRESS} "$@" ${PG_REGRESS_OPTS}
