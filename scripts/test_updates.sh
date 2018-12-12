#!/bin/bash

set -o pipefail

SCRIPT_DIR=$(dirname $0)
TEST_TMPDIR=${TEST_TMPDIR:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_update_test' || mkdir -p /tmp/$RANDOM )}
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
TAGS=${TAGS:-}
TEST_VERSION=${TEST_VERSION:-}
GIT_ID=$(git -C ${BASE_DIR} describe --dirty --always | sed -e "s|/|_|g")
UPDATE_TO_IMAGE=${UPDATE_TO_IMAGE:-update_test}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-${GIT_ID}}
PG_VERSION=${PG_VERSION:-9.6.5} # Need 9.6.x version since we are
                                # upgrading the extension from
                                # versions that didn't support PG10.

FAILED_TEST=

# Declare a hash table to keep test names keyed by pid
declare -A tests

while getopts "c" opt;
do
    case $opt in
        c)
            echo "Forcing cleanup of build image"
            docker rmi -f ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}
            ;;
    esac
done

cleanup() {
    local exit_code="$?"
    set +e # do not exit immediately on failure
    echo "Waiting for remaining tests to finish..."
    wait
    if [ -f ${TEST_TMPDIR}/${FAILED_TEST}.log ]; then
        echo "###### Failed test log below #####"
        cat ${TEST_TMPDIR}/${FAILED_TEST}.log
    fi
    rm -rf ${TEST_TMPDIR}
    echo "exit code is $exit_code"
    return $exit_code
}

kill_all_tests() {
    local exit_code="$?"
    set +e # do not exit immediately on failure
    echo "Killing all tests"
    kill ${!tests[@]} 2>/dev/null
    return $exit_code
}

trap kill_all_tests INT HUP
trap cleanup EXIT

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
    UPDATE_FROM_TAG=${tag} TEST_VERSION=${TEST_VERSION} $(dirname $0)/test_update_from_tag.sh > ${TEST_TMPDIR}/${tag}.log 2>&1 &

    tests[$!]=${tag}
    echo "Launched test ${tag} with pid $!"
done

# Need to wait on each pid in a loop to return the exit status of each
echo "Waiting for tests to finish..."

# Since we are iterating a hash table, the tests are not going to be
# in order started. But it doesn't matter.
for pid in ${!tests[@]};
do
    wait $pid;
    exit_code=$?
    echo "Test ${tests[$pid]} (pid $pid) exited with code $exit_code"

    if [ $exit_code -ne 0 ]; then
        FAILED_TEST=${tests[$pid]}
        kill_all_tests
        exit $exit_code
    fi
    $(exit $exit_code)
done
