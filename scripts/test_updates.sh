#!/bin/bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)
BASE_DIR=${PWD}/${SCRIPT_DIR}/..
PGTEST_TMPDIR=${PGTEST_TMPDIR:-/tmp}
UPDATE_PG_PORT=${UPDATE_PG_PORT:-6432}
CLEAN_PG_PORT=${CLEAN_PG_PORT:-6433}

UPDATE_FROM_IMAGE=${UPDATE_FROM_IMAGE:-timescale/timescaledb}
UPDATE_FROM_TAG=${UPDATE_FROM_TAG:-0.1.0}
UPDATE_TO_IMAGE=${UPDATE_TO_IMAGE:-update_test}
UPDATE_TO_TAG=${UPDATE_TO_TAG:-latest}

wait_for_pg () {
set +e
for i in {1..10}; do
  sleep 2

  docker exec -it $1 /bin/bash -c "pg_isready -U postgres"

  if [[ $? == 0 ]] ; then
    set -e
    return 0
  fi
done
exit 1
}

docker rm -f timescaledb-orig timescaledb-updated timescaledb-clean || true
IMAGE_NAME=update_test TAG_NAME=latest bash scripts/docker-build.sh

docker run -d --name timescaledb-orig -v ${BASE_DIR}:/src ${UPDATE_FROM_IMAGE}:${UPDATE_FROM_TAG}
docker run -d --name timescaledb-clean -v ${BASE_DIR}:/src ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

CLEAN_VOLUME=$(docker inspect timescaledb-clean --format='{{range .Mounts }}{{.Name}}{{end}}')
UPDATE_VOLUME=$(docker inspect timescaledb-orig --format='{{range .Mounts }}{{.Name}}{{end}}')

wait_for_pg timescaledb-orig

echo "Executing setup script on 0.1.0"
docker exec -it timescaledb-orig /bin/bash -c "psql -h localhost -U postgres -f /src/test/sql/updates/setup.sql"
docker rm -f timescaledb-orig

docker run -d --name timescaledb-updated -v ${BASE_DIR}:/src -v ${UPDATE_VOLUME}:/var/lib/postgresql/data ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}

wait_for_pg timescaledb-updated

echo "Executing ALTER EXTENSION timescaledb UPDATE"
docker exec -it timescaledb-updated /bin/bash -c "psql -h localhost -U postgres -d single -c \"ALTER EXTENSION timescaledb UPDATE\""

wait_for_pg timescaledb-clean

echo "Executing setup script on new version"
docker exec -it timescaledb-clean /bin/bash -c "psql -h localhost -U postgres -f /src/test/sql/updates/setup.sql"

echo "Restarting clean container"
#below is needed so the clean container looks like updated, which has been restarted after the setup script
#(especially needed for sequences which might otherwise be in different states -- e.g. some backends may have reserved batches)
docker rm -f timescaledb-clean
docker run -d --name timescaledb-clean -v ${BASE_DIR}:/src -v ${CLEAN_VOLUME}:/var/lib/postgresql/data ${UPDATE_TO_IMAGE}:${UPDATE_TO_TAG}
wait_for_pg timescaledb-clean

echo "Testing"
docker exec timescaledb-updated /bin/bash \
  -c "psql -X -v ECHO=ALL -h localhost -U postgres -d single -f /src/test/sql/updates/test-0.1.1.sql" > ${PGTEST_TMPDIR}/updated.out
docker exec timescaledb-clean /bin/bash \
  -c "psql -X -v ECHO=ALL -h localhost -U postgres -d single -f /src/test/sql/updates/test-0.1.1.sql" > ${PGTEST_TMPDIR}/clean.out

docker rm -vf timescaledb-updated timescaledb-clean

diff ${PGTEST_TMPDIR}/clean.out ${PGTEST_TMPDIR}/updated.out | tee ${PGTEST_TMPDIR}/update_test.output
