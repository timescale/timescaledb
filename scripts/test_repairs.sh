#!/bin/bash

set -o pipefail

SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
GIT_ID=$(git -C ${BASE_DIR} describe --dirty --always | sed -e "s|/|_|g")
TEST_TMPDIR=${TEST_TMPDIR:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_repair_test' || mkdir -p /tmp/${RANDOM})}
UPDATE_TO_IMAGE=${UPDATE_TO_IMAGE:-repair_test}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-${GIT_ID}}

# Build the docker image with current source here so that the parallel
# tests don't all compete in trying to build it first
IMAGE_NAME=${UPDATE_TO_IMAGE} TAG_NAME=${UPDATE_TO_TAG} PG_VERSION=${PG_VERSION} bash ${SCRIPT_DIR}/docker-build.sh

# Run repair tests in parallel
declare -A tests
for tag in ${TAGS}; do
    UPDATE_FROM_TAG=${tag} TEST_VERSION=${TEST_VERSION} bash $SCRIPT_DIR/test_repair_from_tag.sh ${TEST_UPDATE_FROM_TAGS_EXTRA_ARGS} > ${TEST_TMPDIR}/${tag}.log 2>&1 &

    tests[$!]=${tag}
    echo "Launched test ${tag} with pid $!"
done

# Need to wait on each pid in a loop to return the exit status of each
for pid in "${!tests[@]}"; do
    echo "Waiting for test pid $pid"
    wait $pid
    exit_code=$?
    echo "Test ${tests[$pid]} (pid $pid) exited with code $exit_code"

    if [ $exit_code -ne 0 ]; then
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_TEST=${tests[$pid]}
        if [ -f ${TEST_TMPDIR}/${FAILED_TEST}.log ]; then
            echo "###### Failed $UPDATE_TO_TAG test log below #####"
            cat ${TEST_TMPDIR}/${FAILED_TEST}.log
        fi
    fi
    echo "###### test log $UPDATE_TO_TAG below #####"
    cat ${TEST_TMPDIR}/${tests[$pid]}.log
done

if [ "$KEEP_TEMP_DIRS" = "false" ]; then
    echo "Cleaning up temporary directory"
    rm -rf ${TEST_TMPDIR}
fi

exit $FAIL_COUNT

