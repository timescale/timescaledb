#!/bin/bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
PGTEST_TMPDIR=${PGTEST_TMPDIR:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_update_test')}
UPDATE_PG_PORT=${UPDATE_PG_PORT:-6432}
CLEAN_PG_PORT=${CLEAN_PG_PORT:-6433}

UPDATE_FROM_IMAGE=${UPDATE_FROM_IMAGE:-timescale/timescaledb}
UPDATE_FROM_TAG=${UPDATE_FROM_TAG:-0.1.0}
UPDATE_TO_IMAGE=${UPDATE_TO_IMAGE:-update_test}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-latest}
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
    if [ $status -eq 0 ]; then 
        rm -rf ${PGTEST_TMPDIR}
    fi
    docker rm -vf timescaledb-orig timescaledb-clean timescaledb-updated 2>/dev/null
    echo "Exit status is $status"
    exit $status
}

docker_exec() {
    # Echo to stderr
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker exec -it $1 /bin/bash -c "$2"
}

docker_pgcmd() {
    docker_exec $1 "psql -h localhost -U postgres -d single -c \"$2\""
}

docker_pgscript() {
    docker_exec $1 "psql -h localhost -U postgres -f $2"
}

docker_pgtest() {
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker exec $1 psql -X -v ECHO=ALL -v ON_ERROR_STOP=1 -h localhost -U postgres -d single -f $2 > ${PGTEST_TMPDIR}/$1.out || exit $?
}

docker_pgdiff() {
    >&2 echo -e "\033[1m$1 vs $2\033[0m: $2"
    docker_pgtest $1 $3
    docker_pgtest $2 $3
    echo "RUNNING:  diff ${PGTEST_TMPDIR}/$1.out ${PGTEST_TMPDIR}/$2.out "
    diff ${PGTEST_TMPDIR}/$1.out ${PGTEST_TMPDIR}/$2.out | tee ${PGTEST_TMPDIR}/update_test.output
}

docker_run() {
    docker run -d --name $1 -v ${BASE_DIR}:/src $2
    wait_for_pg $1
}

docker_run_vol() {
    docker run -d --name $1 -v ${BASE_DIR}:/src -v $2 $3
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
            sleep 2
            set -e
            return 0
        fi
    done
    exit 1
}

echo "Using temporary directory $PGTEST_TMPDIR"

docker rm -f timescaledb-orig timescaledb-updated timescaledb-clean 2>/dev/null || true
IMAGE_NAME=update_test TAG_NAME=latest bash scripts/docker-build.sh

docker_run timescaledb-orig ${UPDATE_FROM_IMAGE}:${UPDATE_FROM_TAG}
docker_run timescaledb-clean ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

CLEAN_VOLUME=$(docker inspect timescaledb-clean --format='{{range .Mounts }}{{.Name}}{{end}}')
UPDATE_VOLUME=$(docker inspect timescaledb-orig --format='{{range .Mounts }}{{.Name}}{{end}}')

echo "Executing setup script on 0.1.0"
docker_pgscript timescaledb-orig /src/test/sql/updates/setup.sql
docker rm -f timescaledb-orig

docker_run_vol timescaledb-updated ${UPDATE_VOLUME}:/var/lib/postgresql/data ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

echo "Executing ALTER EXTENSION timescaledb UPDATE"
docker_pgcmd timescaledb-updated "ALTER EXTENSION timescaledb UPDATE"

docker_exec timescaledb-updated "pg_dump -h localhost -U postgres -Fc single > /tmp/single.sql"
docker cp timescaledb-updated:/tmp/single.sql ${PGTEST_TMPDIR}/single.sql

echo "Restoring database on new version"
docker cp ${PGTEST_TMPDIR}/single.sql timescaledb-clean:/tmp/single.sql
docker_exec timescaledb-clean "createdb -h localhost -U postgres single"
docker_pgcmd timescaledb-clean "ALTER DATABASE single SET timescaledb.restoring='on'"
docker_exec timescaledb-clean "pg_restore -h localhost -U postgres -d single /tmp/single.sql"
docker_pgcmd timescaledb-clean "ALTER DATABASE single SET timescaledb.restoring='off'"

echo "Testing"
docker_pgdiff timescaledb-updated timescaledb-clean /src/test/sql/updates/test-0.1.1.sql
