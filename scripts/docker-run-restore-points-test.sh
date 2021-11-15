#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
DO_CLEANUP=true

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
    docker rm -vf timescaledb-rp 2>/dev/null
    echo "Exit status is $status"
    exit $status
}

docker_exec() {
    # Echo to stderr
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker exec $1 /bin/bash -c "$2"
}

docker rm -f timescaledb-rp 2>/dev/null || true
IMAGE_NAME=rp_test TAG_NAME=latest bash ${SCRIPT_DIR}/docker-build.sh

# The odd contortion with the BASE_DIR is necessary since SCRIPT_DIR
# is relative and --volume requires an absolute path.
docker run --env TIMESCALEDB_TELEMETRY=off -d \
 --volume ${BASE_DIR}/scripts:/mnt/scripts \
 --name timescaledb-rp rp_test:latest

echo "**** Testing ****"
docker_exec timescaledb-rp "mkdir /testdir"
docker_exec timescaledb-rp "chown postgres:postgres /testdir"
docker_exec timescaledb-rp "(cd /testdir; su postgres sh -c /mnt/scripts/test_restore_points.sh)"
