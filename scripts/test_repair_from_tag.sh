#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..

GIT_ID=$(git -C ${BASE_DIR} describe --dirty --always | sed -e "s|/|_|g")
WITH_SUPERUSER=true # Update tests have superuser privileges when running tests.
UPDATE_FROM_IMAGE=${UPDATE_FROM_IMAGE:-timescale/timescaledb}
UPDATE_FROM_TAG=${UPDATE_FROM_TAG:-0.1.0}
UPDATE_TO_IMAGE=${UPDATE_TO_IMAGE:-update_test}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-${GIT_ID}}

# Extra options to pass to psql
PGOPTS="-v TEST_VERSION=${TEST_VERSION} -v TEST_REPAIR=true -v WITH_SUPERUSER=${WITH_SUPERUSER} -v WITH_ROLES=false"
PSQL="psql -qX $PGOPTS"

DOCKEROPTS="--env TIMESCALEDB_TELEMETRY=off --env POSTGRES_HOST_AUTH_METHOD=trust"

docker_exec() {
    # Echo to stderr
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker exec $1 /bin/bash -c "$2"
}

docker_logs() {
    # Echo to stderr
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker logs $1
}

docker_pgcmd() {
    local database=single
    OPTIND=1
    while getopts "d:" opt; do
        case $opt in
             d)
                 database=$OPTARG
                 ;;
             *)
                 ;;
        esac
    done
    shift $((OPTIND-1))

    echo "executing pgcmd on database $database with container $1"
    set +e
    if ! docker_exec $1 "$PSQL -h localhost -U postgres -d $database -v VERBOSITY=verbose -c \"$2\""; then
      docker_logs $1
      exit 1
    fi
    set -e
}

docker_pgscript() {
    local database=single
    OPTIND=1
    while getopts "d:" opt; do
        case $opt in
             d)
                 database=$OPTARG
                 ;;
             *)
                 ;;
        esac
    done
    shift $((OPTIND-1))

    docker_exec $1 "$PSQL -h localhost -U postgres -d $database -v ON_ERROR_STOP=1 -f $2"
}

docker_run() {
    docker run $DOCKEROPTS -d --name $1 -v ${BASE_DIR}:/src $2 -c timezone='US/Eastern' -c max_prepared_transactions=100
    wait_for_pg $1
}

docker_run_vol() {
    docker run $DOCKEROPTS -d --name $1 -v ${BASE_DIR}:/src -v $2 $3 -c timezone='US/Eastern' -c max_prepared_transactions=100
    wait_for_pg $1
}

wait_for_pg() {
    set +e
    for _ in {1..20}; do
        sleep 1

        if docker_exec $1 "pg_isready -U postgres"; then
            # this makes the test less flaky, although not
            # ideal. Apperently, pg_isready is not always a good
            # indication of whether the DB is actually ready to accept
            # queries
            sleep 5
            set -e
            return 0
        fi
        docker_logs $1

    done
    exit 1
}

CONTAINER_ORIG=timescaledb-orig-$$
CONTAINER_UPDATED=timescaledb-updated-$$

echo "**** Checking repair for update from $UPDATE_FROM_TAG ****"

# Start a container with the correct version
docker_run ${CONTAINER_ORIG} ${UPDATE_FROM_IMAGE}:${UPDATE_FROM_TAG}

UPDATE_VOLUME=$(docker inspect ${CONTAINER_ORIG} --format='{{range .Mounts }}{{.Name}}{{end}}')

docker_pgcmd -d postgres ${CONTAINER_ORIG} "CREATE DATABASE single"
docker_pgscript ${CONTAINER_ORIG} /src/test/sql/updates/setup.repair.sql

# Remove container but keep volume
docker rm -f ${CONTAINER_ORIG}

docker_run_vol ${CONTAINER_UPDATED} ${UPDATE_VOLUME}:/var/lib/postgresql/data ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

docker_pgcmd ${CONTAINER_UPDATED} "ALTER EXTENSION timescaledb UPDATE"
docker_pgscript ${CONTAINER_UPDATED} /src/test/sql/updates/post.repair.sql

# Run an integrity check. It will report if any dimension slices are missing.
docker_pgscript ${CONTAINER_UPDATED} /src/test/sql/updates/post.integrity_test.sql
