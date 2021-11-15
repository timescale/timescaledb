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

# If PROVE_TESTS is specified then run those subset of TAP tests even if
# TESTS is also specified
if [ -z "$PROVE_TESTS" ]
then
    # Exit early if we are running with TESTS=expr
    if [ -n "$TESTS" ]
    then
        exit 0
    fi
    FINAL_TESTS="t/*.pl"
else
    FINAL_TESTS=$PROVE_TESTS
fi

${PROVE} \
    -I "${SRC_DIR}/src/test/perl" \
    -I "${CM_SRC_DIR}/test/perl" \
    -I "${PG_LIBDIR}/pgxs/src/test/perl" \
    -I "${PG_LIBDIR}/postgresql/pgxs/src/test/perl" \
    $FINAL_TESTS
