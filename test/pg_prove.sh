#!/usr/bin/env bash

# Wrapper around perl prove utility to control running of TAP tests
#
# The following control variable is supported:
#
# PROVE_TESTS  only run TAP tests from this list
# e.g make provecheck PROVE_TESTS="t/foo.pl t/bar.pl"
#
# Note that you can also use regular expressions to run multiple
# taps tests matching the pattern:
#
# e.g make provecheck PROVE_TESTS="t/*chunk*"
#

PROVE_TESTS=${PROVE_TESTS:-}
PROVE=${PROVE:-prove}

echo "SKIPS: ${SKIPS}"

# If PROVE_TESTS is specified then run those subset of TAP tests even if
# TESTS is also specified
if [ -z "$PROVE_TESTS" ] && [ -z "${SKIPS}" ]
then
    # Exit early if we are running with TESTS=expr
    if [ -n "$TESTS" ]
    then
        exit 0
    fi
    FINAL_TESTS=$(ls -1 t/*.pl 2>/dev/null)
elif [ -z "$PROVE_TESTS" ] && [ -n "${SKIPS}" ]
then
    ALL_TESTS=$(ls -1 t/*.pl 2>/dev/null)
    FILTERED_TESTS=""
    # disable path expansion to make SKIPS='*' work
    set -f

    # to support wildcards in SKIPS we match the SKIPS
    # list against the actual list of tests
    for test_name in ${ALL_TESTS}; do
      for test_pattern in ${SKIPS}; do
        # shellcheck disable=SC2053
        # We do want to match globs in $test_pattern here.
        if [[ $test_name == t/${test_pattern}.pl ]]; then
          continue 2
        fi
      done
      FILTERED_TESTS="${FILTERED_TESTS}\n${test_name}"
    done
    FINAL_TESTS=$(echo -e "${FILTERED_TESTS}" | tr '\n' ' ' | sed -e 's/^ *//')

else
    FINAL_TESTS=$PROVE_TESTS
fi

if [ -z "$FINAL_TESTS" ]
then
	echo "No TAP tests to run for the current configuration, skipping..."
	exit 0;
fi
PG_VERSION_MAJOR=${PG_VERSION_MAJOR} ${PROVE} \
    -I "${SRC_DIR}/src/test/perl" \
    -I "${CM_SRC_DIR}/test/perl" \
    -I "${PG_LIBDIR}/pgxs/src/test/perl" \
    -I "${PG_PKGLIBDIR}/pgxs/src/test/perl" \
    -I "${PG_LIBDIR}/postgresql/pgxs/src/test/perl" \
    $FINAL_TESTS
