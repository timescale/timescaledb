#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
WITH_SUPERUSER=true # Update tests have superuser privileges when running tests.
TEST_VERSION=${TEST_VERSION:-v2}
TEST_TMPDIR=${TEST_TMPDIR:-$(mktemp -d 2>/dev/null || mktemp -d -t 'timescaledb_downgrade_test' || mkdir -p /tmp/${RANDOM})}
UPDATE_PG_PORT=${UPDATE_PG_PORT:-6432}
CLEAN_PG_PORT=${CLEAN_PG_PORT:-6433}
PG_VERSION=${PG_VERSION:-12.0}
GIT_ID=$(git -C ${BASE_DIR} describe --dirty --always | sed -e "s|/|_|g")
UPDATE_FROM_IMAGE=${UPDATE_FROM_IMAGE:-timescale/timescaledb}
UPDATE_FROM_TAG=${UPDATE_FROM_TAG:-0.1.0}
UPDATE_TO_IMAGE=${UPDATE_TO_IMAGE:-downgrade_test}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-${GIT_ID}}
DO_CLEANUP=${DO_CLEANUP:-true}
PGOPTS="-v TEST_VERSION=${TEST_VERSION} -v TEST_REPAIR=${TEST_REPAIR} -v WITH_SUPERUSER=${WITH_SUPERUSER} -v WITH_ROLES=true -v WITH_CHUNK=true"
GENERATE_DOWNGRADE_SCRIPT=${GENERATE_DOWNGRADE_SCRIPT:-ON}

# The following variables are exported to called scripts.
export GENERATE_DOWNGRADE_SCRIPT PG_VERSION

# PID of the current shell
PID=$$

# Container names. Append shell PID so that we can run this script in parallel
CONTAINER_ORIG=timescaledb-orig-${PID}
CONTAINER_CLEAN_RESTORE=timescaledb-clean-restore-${PID}
CONTAINER_UPDATED=timescaledb-updated-${PID}
CONTAINER_CLEAN_RERUN=timescaledb-clean-rerun-${PID}

export PG_VERSION

if [[ "$DO_CLEANUP" = "false" ]]; then
    echo "!!Debug mode: Containers and temporary directory will be left on disk"
else
    echo "!!Containers and temporary directory will be cleaned up"
fi

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

docker_logs() {
    # Echo to stderr
    >&2 echo -e "\033[1m$1\033[0m: $2"
    docker logs $1
}

docker_pgcmd() {
    local database=${3:-single}
    echo "executing pgcmd on database $database"
    set +e
    if ! docker_exec $1 "psql -h localhost -U postgres -d $database $PGOPTS -v VERBOSITY=verbose -c \"$2\""
    then
      docker_logs $1
      exit 1
    fi
    set -e
}

docker_pgscript() {
    local database=${3:-single}
    docker_exec $1 "psql -h localhost -U postgres -d $database $PGOPTS -v ON_ERROR_STOP=1 -f $2"
}

docker_pgtest() {
    local database=${3:-single}
    set +e
    >&2 echo -e "\033[1m$1\033[0m: $2"
    if ! docker exec $1 psql -X -v ECHO=ALL -v ON_ERROR_STOP=1 -h localhost -U postgres -d $database -f $2 > ${TEST_TMPDIR}/$1.out
    then
      docker_logs $1
      exit 1
    fi
    set -e
}

docker_pgdiff_all() {
    local database=${2:-single}
    diff_file1=downgrade_test.restored.diff.${UPDATE_FROM_TAG}
    diff_file2=downgrade_test.clean.diff.${UPDATE_FROM_TAG}
    docker_pgtest ${CONTAINER_UPDATED} $1 $database
    docker_pgtest ${CONTAINER_CLEAN_RESTORE} $1 $database
    docker_pgtest ${CONTAINER_CLEAN_RERUN} $1 $database
    echo "Diffing downgraded container vs restored. Downgraded: ${CONTAINER_UPDATED} restored: ${CONTAINER_CLEAN_RESTORE}" 
    diff -u ${TEST_TMPDIR}/${CONTAINER_UPDATED}.out ${TEST_TMPDIR}/${CONTAINER_CLEAN_RESTORE}.out | tee ${diff_file1}
    if [ ! -s ${diff_file1} ]; then
      rm ${diff_file1}
    fi
    echo "Diffing downgraded container vs clean run. Downgraded: ${CONTAINER_UPDATED} clean run: ${CONTAINER_CLEAN_RERUN}"
    diff -u ${TEST_TMPDIR}/${CONTAINER_UPDATED}.out ${TEST_TMPDIR}/${CONTAINER_CLEAN_RERUN}.out | tee ${diff_file2}
    if [ ! -s ${diff_file2} ]; then
      rm ${diff_file2}
    fi
}

docker_run() {
    docker run --env TIMESCALEDB_TELEMETRY=off --env POSTGRES_HOST_AUTH_METHOD=trust -d --name $1 -v ${BASE_DIR}:/src $2 -c timezone="GMT" -c max_prepared_transactions=100
    wait_for_pg $1
}

docker_run_vol() {
    docker run --env TIMESCALEDB_TELEMETRY=off --env POSTGRES_HOST_AUTH_METHOD=trust -d --name $1 -v ${BASE_DIR}:/src -v $2 $3 -c timezone="GMT" -c max_prepared_transactions=100
    wait_for_pg $1
}

wait_for_pg() {
    set +e
    for _ in {1..20}; do
        sleep 1

        if docker_exec $1 "pg_isready -U postgres"
        then
            # this makes the test less flaky, although not
            # ideal. Apperently, pg_isready is not always a good
            # indication of whether the DB is actually ready to accept
            # queries
            sleep 1
            set -e
            return 0
        fi
        docker_logs $1

    done
    exit 1
}

# shellcheck disable=SC2001 # SC2001 -- See if you can use ${variable//search/replace} instead.
VERSION=$(echo ${UPDATE_FROM_TAG} | sed 's/\([0-9]\{0,\}\.[0-9]\{0,\}\.[0-9]\{0,\}\).*/\1/g')
echo "Testing from version ${VERSION} (test version ${TEST_VERSION})"
echo "Using temporary directory ${TEST_TMPDIR}"

remove_containers || true

IMAGE_NAME=${UPDATE_TO_IMAGE} TAG_NAME=${UPDATE_TO_TAG} PG_VERSION=${PG_VERSION} bash ${SCRIPT_DIR}/docker-build.sh

echo "Launching containers"
docker_run ${CONTAINER_ORIG} ${UPDATE_FROM_IMAGE}:${UPDATE_FROM_TAG}
docker_run ${CONTAINER_CLEAN_RESTORE} ${UPDATE_FROM_IMAGE}:${UPDATE_FROM_TAG}
docker_run ${CONTAINER_CLEAN_RERUN} ${UPDATE_FROM_IMAGE}:${UPDATE_FROM_TAG}

# Create roles for test. Roles must be created outside of regular
# setup scripts; they must be added separately to each instance since
# roles are not dumped by pg_dump.
docker_pgscript ${CONTAINER_ORIG} /src/test/sql/updates/setup.roles.sql "postgres"
docker_pgscript ${CONTAINER_CLEAN_RESTORE} /src/test/sql/updates/setup.roles.sql "postgres"
docker_pgscript ${CONTAINER_CLEAN_RERUN} /src/test/sql/updates/setup.roles.sql "postgres"

CLEAN_VOLUME=$(docker inspect ${CONTAINER_CLEAN_RESTORE} --format='{{range .Mounts }}{{.Name}}{{end}}')
UPDATE_VOLUME=$(docker inspect ${CONTAINER_ORIG} --format='{{range .Mounts }}{{.Name}}{{end}}')

echo "Executing setup script on container running ${UPDATE_FROM_IMAGE}:${UPDATE_FROM_TAG}"
docker_pgscript ${CONTAINER_ORIG} /src/test/sql/updates/setup.databases.sql "postgres"
docker_pgscript ${CONTAINER_ORIG} /src/test/sql/updates/pre.testing.sql
docker_pgscript ${CONTAINER_ORIG} /src/test/sql/updates/setup.${TEST_VERSION}.sql
docker_pgcmd ${CONTAINER_ORIG} "CHECKPOINT;"

# We need the previous version shared libraries as well, so we copy
# all shared libraries out from the original container before stopping
# it. We could limit it to just the preceeding version, but this is
# more straightforward.
srcdir=$(docker exec ${CONTAINER_ORIG} /bin/bash -c 'pg_config --pkglibdir')
FILES=$(docker exec ${CONTAINER_ORIG} /bin/bash -c "ls $srcdir/timescaledb*.so")
for file in $FILES; do
    docker cp "${CONTAINER_ORIG}:$file" "${TEST_TMPDIR}/$(basename $file)"
done

# Remove container but keep volume
docker rm -f ${CONTAINER_ORIG}

echo "Running downgraded container"
docker_run_vol ${CONTAINER_UPDATED} ${UPDATE_VOLUME}:/var/lib/postgresql/data ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

dstdir=$(docker exec ${CONTAINER_UPDATED} /bin/bash -c 'pg_config --pkglibdir')
for file in $FILES; do
    docker cp "${TEST_TMPDIR}/$(basename $file)" "${CONTAINER_UPDATED}:$dstdir"
    rm "${TEST_TMPDIR}/$(basename $file)"
done

echo "==== 1. check caggs ===="
docker_pgcmd ${CONTAINER_UPDATED} "SELECT user_view_schema, user_view_name FROM _timescaledb_catalog.continuous_agg"

echo "Executing ALTER EXTENSION timescaledb UPDATE for update ($UPDATE_FROM_TAG -> $UPDATE_TO_TAG)"
docker_pgcmd ${CONTAINER_UPDATED} "ALTER EXTENSION timescaledb UPDATE" "single"
docker_pgcmd ${CONTAINER_UPDATED} "ALTER EXTENSION timescaledb UPDATE" "dn1"
# Need to update also postgres DB since add_data_node may connect to
# it and it will be borked if we don't upgrade to an extension binary
# which is available in the image.
docker_pgcmd ${CONTAINER_UPDATED} "ALTER EXTENSION timescaledb UPDATE" "postgres"

echo "==== 2. check caggs ===="
docker_pgcmd ${CONTAINER_UPDATED} "SELECT user_view_schema, user_view_name FROM _timescaledb_catalog.continuous_agg"

# We now assume for some reason the user wanted to downgrade, so we
# downgrade the just upgraded version.
echo "Executing ALTER EXTENSION timescaledb UPDATE for downgrade ($UPDATE_TO_TAG -> $UPDATE_FROM_TAG)"
docker_pgcmd ${CONTAINER_UPDATED} "ALTER EXTENSION timescaledb UPDATE TO '$VERSION'" "postgres"
docker_pgcmd ${CONTAINER_UPDATED} "ALTER EXTENSION timescaledb UPDATE TO '$VERSION'" "dn1"
docker_pgcmd ${CONTAINER_UPDATED} "ALTER EXTENSION timescaledb UPDATE TO '$VERSION'" "single"

# Check that there is nothing wrong before taking a backup
echo "Checking that there are no missing dimension slices"
docker_pgscript ${CONTAINER_UPDATED} /src/test/sql/updates/setup.check.sql

# Code below is similar to how it works for update scripts, but here
# we run it on the downgraded version instead.

echo "Executing setup script on clean"
docker_pgscript ${CONTAINER_CLEAN_RERUN} /src/test/sql/updates/setup.databases.sql "postgres"
docker_pgscript ${CONTAINER_CLEAN_RERUN} /src/test/sql/updates/pre.testing.sql
docker_pgscript ${CONTAINER_CLEAN_RERUN} /src/test/sql/updates/setup.${TEST_VERSION}.sql
docker_pgscript ${CONTAINER_CLEAN_RERUN} /src/test/sql/updates/setup.post-downgrade.sql

docker_exec ${CONTAINER_UPDATED} "pg_dump -h localhost -U postgres -Fc single > /tmp/single.dump"
docker_exec ${CONTAINER_UPDATED} "pg_dump -h localhost -U postgres -Fc dn1 > /tmp/dn1.dump"
docker cp ${CONTAINER_UPDATED}:/tmp/single.dump ${TEST_TMPDIR}/single.dump
docker cp ${CONTAINER_UPDATED}:/tmp/dn1.dump ${TEST_TMPDIR}/dn1.dump

echo "Restoring database on clean version"
docker cp ${TEST_TMPDIR}/single.dump ${CONTAINER_CLEAN_RESTORE}:/tmp/single.dump
docker cp ${TEST_TMPDIR}/dn1.dump ${CONTAINER_CLEAN_RESTORE}:/tmp/dn1.dump

# Restore single
docker_exec ${CONTAINER_CLEAN_RESTORE} "createdb -h localhost -U postgres single"
docker_pgcmd ${CONTAINER_CLEAN_RESTORE} "ALTER DATABASE single SET timescaledb.restoring='on'"
docker_exec ${CONTAINER_CLEAN_RESTORE} "pg_restore -h localhost -U postgres -d single /tmp/single.dump"
docker_pgcmd ${CONTAINER_CLEAN_RESTORE} "ALTER DATABASE single RESET timescaledb.restoring"

# Restore dn1
docker_exec ${CONTAINER_CLEAN_RESTORE} "createdb -h localhost -U postgres dn1"
docker_pgcmd ${CONTAINER_CLEAN_RESTORE} "ALTER DATABASE dn1 SET timescaledb.restoring='on'"
docker_exec ${CONTAINER_CLEAN_RESTORE} "pg_restore -h localhost -U postgres -d dn1 /tmp/dn1.dump"
docker_pgcmd ${CONTAINER_CLEAN_RESTORE} "ALTER DATABASE dn1 RESET timescaledb.restoring"

echo "Comparing downgraded ($UPDATE_FROM_TAG -> $UPDATE_FROM_TAG) with clean install ($UPDATE_FROM_TAG)"
docker_pgdiff_all /src/test/sql/updates/post.${TEST_VERSION}.sql "single"
