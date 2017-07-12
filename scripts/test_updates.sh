#!/bin/bash

set -e
set -o pipefail

PGTEST_TMPDIR=${PGTEST_TMPDIR:-/tmp}
UPDATE_PG_PORT=${UPDATE_PG_PORT:-6432}
CLEAN_PG_PORT=${CLEAN_PG_PORT:-6433}

UPDATE_FROM_TAG=${UPDATE_FROM_TAG:-0.1.0}

wait_for_pg () {
set +e
for i in {1..10}; do
  sleep 2

  pg_isready -h localhost -p $1

  if [[ $? == 0 ]] ; then
    set -e
    return 0
  fi
done
exit 1
}

docker rm -f timescaledb-orig timescaledb-updated timescaledb-clean || true
rm -rf  ${PGTEST_TMPDIR}/pg_data ${PGTEST_TMPDIR}/pg_data_clean
IMAGE_NAME=update_test TAG_NAME=latest bash scripts/docker-build.sh

docker run -d --name timescaledb-clean -v /tmp/pg_data_clean:/var/lib/postgresql/data -p ${CLEAN_PG_PORT}:5432 update_test:latest
docker run -d --name timescaledb-orig -v ${PGTEST_TMPDIR}/pg_data:/var/lib/postgresql/data -p ${UPDATE_PG_PORT}:5432 timescale/timescaledb:${UPDATE_FROM_TAG}

wait_for_pg ${UPDATE_PG_PORT}

echo "Executing setup script on 0.1.0"
psql -h localhost -U postgres -p ${UPDATE_PG_PORT} -f test/sql/updates/setup.sql
docker rm -vf timescaledb-orig

docker run -d --name timescaledb-updated -v /tmp/pg_data:/var/lib/postgresql/data -p ${UPDATE_PG_PORT}:5432 update_test:latest

wait_for_pg ${UPDATE_PG_PORT}

echo "Executing ALTER EXTENSION timescaledb UPDATE"
psql -h localhost -U postgres -d single -p ${UPDATE_PG_PORT} -c "ALTER EXTENSION timescaledb UPDATE"


wait_for_pg ${CLEAN_PG_PORT}

echo "Executing setup script on new version"
psql -h localhost -U postgres -p ${CLEAN_PG_PORT} -f test/sql/updates/setup.sql

echo "Restarting clean container"
#below is needed so the clean container looks like updated, which has been restarted after the setup script
#(especially needed for sequences which might otherwise be in different states -- e.g. some backends may have reserved batches)
docker rm -vf timescaledb-clean
docker run -d --name timescaledb-clean -v /tmp/pg_data_clean:/var/lib/postgresql/data -p ${CLEAN_PG_PORT}:5432 update_test:latest
wait_for_pg ${CLEAN_PG_PORT}

echo "Testing"
psql -X -v ECHO=ALL -h localhost -U postgres -d single -p ${UPDATE_PG_PORT} -f test/sql/updates/test-0.1.1.sql > /tmp/updated.out
psql -X -v ECHO=ALL -h localhost -U postgres -d single -p ${CLEAN_PG_PORT} -f test/sql/updates/test-0.1.1.sql > /tmp/clean.out

docker rm -f timescaledb-updated timescaledb-clean || rm -rf  ${PGTEST_TMPDIR}/pg_data ${PGTEST_TMPDIR}/pg_data_clean

diff ${PGTEST_TMPDIR}/clean.out ${PGTEST_TMPDIR}/updated.out | tee ${PGTEST_TMPDIR}/update_test.output
