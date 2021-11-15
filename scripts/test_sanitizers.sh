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
    	*)
    		echo "Unknown flag '$opt'"
    		exit 1
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
        # only print respective postmaster.log when regression.diffs exists
        docker_exec timescaledb-san "if [ -f /tsdb_build/timescaledb/build/test/regression.diffs ]; then cat /tsdb_build/timescaledb/build/test/regression.diffs /tsdb_build/timescaledb/build/test/log/postmaster.log; fi"
        docker_exec timescaledb-san "if [ -f /tsdb_build/timescaledb/build/tsl/test/regression.diffs ]; then cat /tsdb_build/timescaledb/build/tsl/test/regression.diffs /tsdb_build/timescaledb/build/tsl/test/log/postmaster.log; fi"
    fi

    echo "Exit status is $status"
    exit $status
}

docker_exec() {
    # Echo to stderr
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker exec $1 /bin/bash -c "$2"
}

docker rm -f timescaledb-san 2>/dev/null || true

docker run -d --privileged --name timescaledb-san --env POSTGRES_HOST_AUTH_METHOD=trust -v ${TIMESCALE_DIR}:/timescaledb ${REMOTE_ORG}/${REMOTE_NAME}:${REMOTE_TAG}

# Run these commands as root to copy the source into the
# container. Make sure that all files in the copy is owned by user
# 'postgres', which we use to run tests below.
docker exec -i timescaledb-san /bin/bash -Oe <<EOF
mkdir /tsdb_build
chown postgres /tsdb_build
cp -R /timescaledb tsdb_build
chown -R postgres:postgres /tsdb_build
EOF

# Build TimescaleDB as 'postgres' user
docker exec -i -u postgres -w /tsdb_build/timescaledb timescaledb-san /bin/bash -Oe <<EOF
export CFLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer -O2"
export PG_SOURCE_DIR="/usr/src/postgresql/"
export BUILD_FORCE_REMOVE=true
./bootstrap -DCMAKE_BUILD_TYPE='Debug' -DTEST_GROUP_SIZE=1
cd build
make
EOF

# Install TimescaleDB as root
docker exec -i -w /tsdb_build/timescaledb/build timescaledb-san /bin/bash <<EOF
make install
EOF

echo "Testing"

# Echo to stderr
>&2 echo -e "\033[1m$1\033[0m: $2"

# Run tests as 'postgres' user.
#
# IGNORE some test since they fail under ASAN. At least the remote_txn
# test seems to fail due to a PostgreSQL bug where AbortStartTime in
# postmaster.c is not atomic but read/written across signal handlers
# and ServerLoop.
docker exec -i -u postgres -w /tsdb_build/timescaledb/build timescaledb-san /bin/bash <<EOF
make -k regresscheck regresscheck-t SKIPS='remote_txn' IGNORES='bgw_db_scheduler bgw_launcher cluster-11 continuous_aggs_ddl-11'
EOF
