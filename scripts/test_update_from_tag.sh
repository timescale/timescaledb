#!/bin/bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
TEST_VERSION=${TEST_VERSION:-v2}
PGTEST_TMPDIR=${PGTEST_TMPDIR:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_update_test')}
UPDATE_PG_PORT=${UPDATE_PG_PORT:-6432}
CLEAN_PG_PORT=${CLEAN_PG_PORT:-6433}
PG_VERSION=${PG_VERSION:-9.6.5} # Need 9.6.x version since we are
                                # upgrading the extension from
                                # versions that didn't support PG10.
UPDATE_FROM_IMAGE=${UPDATE_FROM_IMAGE:-timescale/timescaledb}
UPDATE_FROM_TAG=${UPDATE_FROM_TAG:-0.1.0}
UPDATE_TO_IMAGE=${UPDATE_TO_IMAGE:-update_test}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-latest}
DO_CLEANUP=true

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
        docker rm -vf timescaledb-orig timescaledb-clean-restore timescaledb-updated 2>/dev/null
    fi
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
    docker_exec $1 "psql -h localhost -U postgres -v ON_ERROR_STOP=1 -f $2"
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
    docker run -d --name $1 -v ${BASE_DIR}:/src $2 -c timezone="US/Eastern"
    wait_for_pg $1
}

docker_run_vol() {
    docker run -d --name $1 -v ${BASE_DIR}:/src -v $2 $3 -c timezone="US/Eastern"
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

VERSION=`echo ${UPDATE_FROM_TAG} | sed 's/\([0-9]\{0,\}\.[0-9]\{0,\}\.[0-9]\{0,\}\).*/\1/g'`
echo "Testing from version ${VERSION} (test version ${TEST_VERSION})"
echo "Using temporary directory $PGTEST_TMPDIR"

docker rm -f timescaledb-orig timescaledb-updated timescaledb-clean-restore timescaledb-clean-rerun 2>/dev/null || true
IMAGE_NAME=update_test TAG_NAME=latest bash ${SCRIPT_DIR}/docker-build.sh

docker_run timescaledb-orig ${UPDATE_FROM_IMAGE}:${UPDATE_FROM_TAG}
docker_run timescaledb-clean-restore ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}
docker_run timescaledb-clean-rerun ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

CLEAN_VOLUME=$(docker inspect timescaledb-clean-restore --format='{{range .Mounts }}{{.Name}}{{end}}')
UPDATE_VOLUME=$(docker inspect timescaledb-orig --format='{{range .Mounts }}{{.Name}}{{end}}')

echo "Executing setup script on ${VERSION}"
docker_pgscript timescaledb-orig /src/test/sql/updates/setup.${TEST_VERSION}.sql
docker rm -f timescaledb-orig

docker_run_vol timescaledb-updated ${UPDATE_VOLUME}:/var/lib/postgresql/data ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

echo "Executing ALTER EXTENSION timescaledb UPDATE"
docker_pgcmd timescaledb-updated "ALTER EXTENSION timescaledb UPDATE"

docker_exec timescaledb-updated "pg_dump -h localhost -U postgres -Fc single > /tmp/single.sql"
docker cp timescaledb-updated:/tmp/single.sql ${PGTEST_TMPDIR}/single.sql

echo "Executing setup script on clean"
docker_pgscript timescaledb-clean-rerun /src/test/sql/updates/setup.${TEST_VERSION}.sql

echo "Testing updated vs clean"
docker_pgdiff timescaledb-updated timescaledb-clean-rerun /src/test/sql/updates/test-rerun.sql

echo "Restoring database on clean version"
docker cp ${PGTEST_TMPDIR}/single.sql timescaledb-clean-restore:/tmp/single.sql
docker_exec timescaledb-clean-restore "createdb -h localhost -U postgres single"
docker_pgcmd timescaledb-clean-restore "ALTER DATABASE single SET timescaledb.restoring='on'"
docker_exec timescaledb-clean-restore "pg_restore -h localhost -U postgres -d single /tmp/single.sql"
docker_pgcmd timescaledb-clean-restore "ALTER DATABASE single SET timescaledb.restoring='off'"

echo "Testing restored"
docker_pgdiff timescaledb-updated timescaledb-clean-restore /src/test/sql/updates/post.${TEST_VERSION}.sql
