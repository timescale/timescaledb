#!/bin/bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
DO_CLEANUP=true

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
    # docker rm -vf timescaledb-valgrind 2>/dev/null
    docker rm -vf timescaledb-memory 2>/dev/null
    echo "Exit status is $status"
    exit $status
}

docker_exec() {
    # Echo to stderr
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker exec -it $1 /bin/bash -c "$2"
}

docker_run() {
    docker run -d --name $1 $2
    wait_for_pg $1
}

wait_for_pg() {
    set +e
    for i in {1..10}; do
        sleep 2

        docker_exec $1 "pg_isready -U postgres"

        if [[ $? == 0 ]] ; then
            # this makes the test less flaky, although not
            # ideal. Apperently, pg_isready is not always a good
            # indication of whether the DB is actually ready to accept
            # queries
            sleep 5
            set -e
            return 0
        fi
    done
    exit 1
}


docker rm -f timescaledb-memory 2>/dev/null || true
IMAGE_NAME=memory_test TAG_NAME=latest bash ${SCRIPT_DIR}/docker-build.sh

docker_run timescaledb-memory memory_test:latest

echo "Installing python3 and psutil"
docker_exec timescaledb-memory "apk add --no-cache python3 && python3 -m ensurepip && pip3 install --upgrade pip && apk add --update build-base python3-dev py-psutil"

echo "Copying necessary scripts from host to docker container"
docker cp -a /tmp/tsdb-dev-tools timescaledb-memory:/tmp/tsdb-dev-tools

echo "Testing"
docker_exec timescaledb-memory "python3 /tmp/tsdb-dev-tools/test_memory_spikes.py & sleep 5 && psql -U postgres -d postgres -h localhost -v ECHO=all -X -f /tmp/tsdb-dev-tools/sql/out_of_order_random_direct.sql"
