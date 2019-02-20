#!/bin/bash

set -e
set -o pipefail

DO_CLEANUP=true
SCRIPT_DIR=${SCRIPT_DIR:-$(dirname $0)}
EXCLUDE_PATTERN=${EXCLUDE_PATTERN:-'^$'} # tests matching regex pattern will be excluded
INCLUDE_PATTERN=${INCLUDE_PATTERN:-'.*'} # tests matching regex pattern will be included
TEST_MAX=${TEST_MAX:-$((2**16))}
TEST_MIN=${TEST_MIN:-$((-1))}
USE_REMOTE=${USE_REMOTE:-false}
REMOTE_TAG=${REMOTE_TAG:-'latest'}
PUSH_IMAGE=${PUSH_IMAGE:-false}
REMOTE_ORG=${REMOTE_ORG:-'timescaledev'}
REMOTE_NAME=${REMOTE_NAME:-'postgres-dev-clang'}
TIMESCALE_DIR=${TIMESCALE_DIR:-${PWD}/${SCRIPT_DIR}/..}

while getopts "d" opt;
do
    case $opt in
        d)
            DO_CLEANUP=false
            echo "!!Debug mode: Containers and temporary directory will be left on disk"
            echo
            ;;
    esac
done

shift $((OPTIND-1))

if "$DO_CLEANUP" = "true"; then
    trap cleanup EXIT
fi

cleanup() {
    # Save status here so that we can return the status of the last
    # command in the script and not the last command of the cleanup
    # function
    status="$?"
    set +e # do not exit immediately on failure in cleanup handler

    if [[ $status -eq 0 ]]; then
        echo "All tests passed"
        docker rm -vf timescaledb-san 2>/dev/null
    else
        # docker logs timescaledb-san
        docker_exec timescaledb-san "cat /tsdb_build/timescaledb/build/test/regression.diffs"
        docker_exec timescaledb-san "cat /tsdb_build/timescaledb/build/test/log/postmaster.log"
    fi

    echo "Exit status is $status"
    exit $status
}

docker_exec() {
    # Echo to stderr
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker exec -it $1 /bin/bash -c "$2"
}

wait_for_pg() {
    set +e
    for i in {1..30}; do
        sleep 2

        docker_exec $1 "pg_isready -U postgres"

        if [[ $? == 0 ]] ; then
            # this makes the test less flaky, although not
            # ideal. Apperently, pg_isready is not always a good
            # indication of whether the DB is actually ready to accept
            # queries
            sleep 1
            set -e
            return 0
        fi
    done
    exit 1
}


docker rm -f timescaledb-san 2>/dev/null || true

docker run -d --privileged --name timescaledb-san -v ${TIMESCALE_DIR}:/timescaledb ${REMOTE_ORG}/${REMOTE_NAME}:${REMOTE_TAG}

docker exec timescaledb-san /bin/bash -c "mkdir /tsdb_build && chown postgres /tsdb_build"

docker exec -u postgres timescaledb-san /bin/bash -c "cp -R /timescaledb tsdb_build"

docker exec -u postgres \
    -e CFLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
    -e CMAKE_BUILD_TYPE="Debug" \
    -e PG_SOURCE_DIR="/usr/src/postgresql/" \
    timescaledb-san /bin/bash -c \
    "cd /tsdb_build/timescaledb && BUILD_FORCE_REMOVE=true ./bootstrap -DTEST_GROUP_SIZE=1 && cd build && make"

wait_for_pg timescaledb-san

docker exec timescaledb-san /bin/bash -c "cd /tsdb_build/timescaledb/build && make install"

echo "Testing"

# Echo to stderr
>&2 echo -e "\033[1m$1\033[0m: $2"
docker exec -u postgres \
    timescaledb-san /bin/bash -c \
    "cd /tsdb_build/timescaledb/build \
        && PATH=\$PATH make regresscheck regresscheck-t"
