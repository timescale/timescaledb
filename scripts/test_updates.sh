#!/bin/bash

set -o pipefail
set +e # Should not exit immediately on failure

SCRIPT_DIR=$(dirname $0)
TEST_TMPDIR=${TEST_TMPDIR:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_update_test' || mkdir -p /tmp/$RANDOM )}
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
TAGS=${TAGS:-}
TEST_VERSION=${TEST_VERSION:-}
GIT_ID=$(git -C ${BASE_DIR} describe --dirty --always | sed -e "s|/|_|g")
UPDATE_TO_IMAGE=${UPDATE_TO_IMAGE:-update_test}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-${GIT_ID}}
PG_VERSION=${PG_VERSION:-12.0}

# This will propagate to the test_update_from_tags.sh script
export TEST_REPAIR

FAILED_TEST=
KEEP_TEMP_DIRS=false
TEST_UPDATE_FROM_TAGS_EXTRA_ARGS=
TEST_REPAIR=false
FAIL_COUNT=0

# Declare a hash table to keep test names keyed by pid
declare -A tests

while getopts "cd" opt;
do
    case $opt in
        c)
            echo "Forcing cleanup of build image"
            docker rmi -f ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}
            ;;
        d)
            echo "Keeping temporary directory ${TEST_TMPDIR}"
            KEEP_TEMP_DIRS=true
            TEST_UPDATE_FROM_TAGS_EXTRA_ARGS="-d"
            ;;
    	*)
    		echo "Unknown flag '$opt'"
    		exit 1
    		;;
    esac
done

kill_all_tests() {
    local exit_code="$?"
    set +e # do not exit immediately on failure
    echo "Killing all tests"
    kill "${!tests[@]}" 2>/dev/null
    return $exit_code
}

trap kill_all_tests INT HUP

if [ -z "${TEST_VERSION}" ]; then
    echo "No TEST_VERSION specified"
    exit 1
fi

if [ -z "${TAGS}" ]; then
    echo "No TAGS specified"
    exit 1
fi

# Build the docker image with current source here so that the parallel
# tests don't all compete in trying to build it first
IMAGE_NAME=${UPDATE_TO_IMAGE} TAG_NAME=${UPDATE_TO_TAG} PG_VERSION=${PG_VERSION} bash ${SCRIPT_DIR}/docker-build.sh

# Run update tests in parallel
for tag in ${TAGS};
do
    UPDATE_FROM_TAG=${tag} TEST_VERSION=${TEST_VERSION} "$(dirname $0)/test_update_from_tag.sh" ${TEST_UPDATE_FROM_TAGS_EXTRA_ARGS} > ${TEST_TMPDIR}/${tag}.log 2>&1 &

    tests[$!]=${tag}
    echo "Launched test ${tag} with pid $!"
done

# Need to wait on each pid in a loop to return the exit status of each

# Since we are iterating a hash table, the tests are not going to be
# in order started. But it doesn't matter.
for pid in "${!tests[@]}"
do
    echo "Waiting for test pid $pid"
    wait $pid
    exit_code=$?
    echo "Test ${tests[$pid]} (pid $pid) exited with code $exit_code"

    if [ $exit_code -ne 0 ]; then
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_TEST=${tests[$pid]}
        if [ -f ${TEST_TMPDIR}/${FAILED_TEST}.log ]; then
            echo "###### Failed test log below #####"
            cat ${TEST_TMPDIR}/${FAILED_TEST}.log
        fi
    fi
done

if [ "$KEEP_TEMP_DIRS" = "false" ]; then
    echo "Cleaning up temporary directory"
    rm -rf ${TEST_TMPDIR}
fi

exit $FAIL_COUNT
