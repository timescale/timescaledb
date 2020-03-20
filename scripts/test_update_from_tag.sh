#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
TEST_VERSION=${TEST_VERSION:-v2}
TEST_TMPDIR=${TEST_TMPDIR:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_update_test' || mkdir -p /tmp/${RANDOM})}
UPDATE_PG_PORT=${UPDATE_PG_PORT:-6432}
CLEAN_PG_PORT=${CLEAN_PG_PORT:-6433}
PG_VERSION=${PG_VERSION:-9.6.5} # Need 9.6.x version since we are
                                # upgrading the extension from
                                # versions that didn't support PG10.
GIT_ID=$(git -C ${BASE_DIR} describe --dirty --always | sed -e "s|/|_|g")
UPDATE_FROM_IMAGE=${UPDATE_FROM_IMAGE:-timescale/timescaledb}
UPDATE_FROM_TAG=${UPDATE_FROM_TAG:-0.1.0}
UPDATE_TO_IMAGE=${UPDATE_TO_IMAGE:-update_test}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-${GIT_ID}}
DO_CLEANUP=true

# PID of the current shell
PID=$$

# Container names. Append shell PID so that we can run this script in parallel
CONTAINER_ORIG=timescaledb-orig-${PID}
CONTAINER_CLEAN_RESTORE=timescaledb-clean-restore-${PID}
CONTAINER_UPDATED=timescaledb-updated-${PID}
CONTAINER_CLEAN_RERUN=timescaledb-clean-rerun-${PID}

export PG_VERSION

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

trap cleanup EXIT

remove_containers() {
    docker rm -vf ${CONTAINER_ORIG} 2>/dev/null
    docker rm -vf ${CONTAINER_CLEAN_RESTORE} 2>/dev/null
    docker rm -vf ${CONTAINER_UPDATED}  2>/dev/null
    docker rm -vf ${CONTAINER_CLEAN_RERUN} 2>/dev/null
    docker volume rm -f ${CLEAN_VOLUME} 2>/dev/null
    docker volume rm -f ${UPDATE_VOLUME} 2>/dev/null
}

cleanup() {
    # Save status here so that we can return the status of the last
    # command in the script and not the last command of the cleanup
    # function
    local status="$?"
    set +e # do not exit immediately on failure in cleanup handler
    if [ "$DO_CLEANUP" = "true" ]; then
        rm -rf ${TEST_TMPDIR}
        sleep 1
        remove_containers
    fi
    echo "Test with pid ${PID} exited with code ${status}"
    exit ${status}
}

docker_exec() {
    # Echo to stderr
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker exec $1 /bin/bash -c "$2"
}

docker_pgcmd() {
    docker_exec $1 "psql -h localhost -U postgres -d single -c \"$2\""
}

docker_pgscript() {
    docker_exec $1 "psql -h localhost -U postgres -v ON_ERROR_STOP=1 -f $2"
}

docker_pgtest() {
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker exec $1 psql -X -v ECHO=ALL -v ON_ERROR_STOP=1 -h localhost -U postgres -d single -f $2 > ${TEST_TMPDIR}/$1.out || exit $?
}

docker_pgdiff() {
    >&2 echo -e "\033[1m$1 vs $2\033[0m: $2"
    docker_pgtest $1 $3
    docker_pgtest $2 $3
    echo "RUNNING:  diff ${TEST_TMPDIR}/$1.out ${TEST_TMPDIR}/$2.out "
    diff -u ${TEST_TMPDIR}/$1.out ${TEST_TMPDIR}/$2.out | tee ${TEST_TMPDIR}/update_test.output
}

docker_run() {
    docker run --env TIMESCALEDB_TELEMETRY=off --env POSTGRES_HOST_AUTH_METHOD=trust -d --name $1 -v ${BASE_DIR}:/src $2 -c timezone="US/Eastern"
    wait_for_pg $1
}

docker_run_vol() {
    docker run --env TIMESCALEDB_TELEMETRY=off --env POSTGRES_HOST_AUTH_METHOD=trust -d --name $1 -v ${BASE_DIR}:/src -v $2 $3 -c timezone="US/Eastern"
    wait_for_pg $1
}

wait_for_pg() {
    set +e
    for i in {1..20}; do
        sleep 0.5

        docker_exec $1 "pg_isready -U postgres"

        if [[ $? == 0 ]] ; then
            # this makes the test less flaky, although not
            # ideal. Apperently, pg_isready is not always a good
            # indication of whether the DB is actually ready to accept
            # queries
            sleep 0.2
            set -e
            return 0
        fi
    done
    exit 1
}

VERSION=`echo ${UPDATE_FROM_TAG} | sed 's/\([0-9]\{0,\}\.[0-9]\{0,\}\.[0-9]\{0,\}\).*/\1/g'`
echo "Testing from version ${VERSION} (test version ${TEST_VERSION})"
echo "Using temporary directory ${TEST_TMPDIR}"

remove_containers || true

IMAGE_NAME=${UPDATE_TO_IMAGE} TAG_NAME=${UPDATE_TO_TAG} PG_VERSION=${PG_VERSION} bash ${SCRIPT_DIR}/docker-build.sh

docker_run ${CONTAINER_ORIG} ${UPDATE_FROM_IMAGE}:${UPDATE_FROM_TAG}
docker_run ${CONTAINER_CLEAN_RESTORE} ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}
docker_run ${CONTAINER_CLEAN_RERUN} ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

CLEAN_VOLUME=$(docker inspect ${CONTAINER_CLEAN_RESTORE} --format='{{range .Mounts }}{{.Name}}{{end}}')
UPDATE_VOLUME=$(docker inspect ${CONTAINER_ORIG} --format='{{range .Mounts }}{{.Name}}{{end}}')

echo "Executing setup script on ${VERSION}"
docker_pgscript ${CONTAINER_ORIG} /src/test/sql/updates/setup.${TEST_VERSION}.sql

# Remove container but keep volume
docker rm -f ${CONTAINER_ORIG}

echo "Running update container"
docker_run_vol ${CONTAINER_UPDATED} ${UPDATE_VOLUME}:/var/lib/postgresql/data ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

echo "Executing ALTER EXTENSION timescaledb UPDATE"
docker_pgcmd ${CONTAINER_UPDATED} "ALTER EXTENSION timescaledb UPDATE"

docker_exec ${CONTAINER_UPDATED} "pg_dump -h localhost -U postgres -Fc single > /tmp/single.sql"
docker cp ${CONTAINER_UPDATED}:/tmp/single.sql ${TEST_TMPDIR}/single.sql

echo "Executing setup script on clean"
docker_pgscript ${CONTAINER_CLEAN_RERUN} /src/test/sql/updates/setup.${TEST_VERSION}.sql

echo "Testing updated vs clean"
docker_pgdiff ${CONTAINER_UPDATED} ${CONTAINER_CLEAN_RERUN} /src/test/sql/updates/test-rerun.sql

echo "Restoring database on clean version"
docker cp ${TEST_TMPDIR}/single.sql ${CONTAINER_CLEAN_RESTORE}:/tmp/single.sql
docker_exec ${CONTAINER_CLEAN_RESTORE} "createdb -h localhost -U postgres single"
docker_pgcmd ${CONTAINER_CLEAN_RESTORE} "ALTER DATABASE single SET timescaledb.restoring='on'"
docker_exec ${CONTAINER_CLEAN_RESTORE} "pg_restore -h localhost -U postgres -d single /tmp/single.sql"
docker_pgcmd ${CONTAINER_CLEAN_RESTORE} "ALTER DATABASE single SET timescaledb.restoring='off'"

echo "Testing restored"
docker_pgdiff ${CONTAINER_UPDATED} ${CONTAINER_CLEAN_RESTORE} /src/test/sql/updates/post.${TEST_VERSION}.sql
