#!/usr/bin/env bash
#
# Wrapper for the PostgreSQL isolationtest runner. It replaces
# the chunks IDs in the output of the tests by _X_X_. So, even
# if the IDs change, the tests will not fail.
##############################################################

set -e
set -u
CURRENT_DIR=$(dirname $0)

ISOLATIONTEST=$1
shift

# Note that removing the chunk numbers is not enough. The chunk numbers also
# influence the alignment of the EXPLAIN output, so not only we have to replace
# them, we also have to remove the "----"s and the trailing spaces. The aligned
# output format in isolation tester is hardcoded, we cannot change it. Moreover,
# the chunk numbers influence the names of indexes if they are long enough to be
# truncated, so the only way to get a stable explain output is to run such a test
# in a separate database.
$ISOLATIONTEST "$@" | ${CURRENT_DIR}/runner_cleanup_output.sh "isolation"
