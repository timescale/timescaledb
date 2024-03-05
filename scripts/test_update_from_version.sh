#!/usr/bin/env bash

# During the update test the following databases will be created:
# - baseline: fresh installation of $TO_VERSION
# - updated: install $FROM_VERSION, update to $TO_VERSION
# - restored: restore from updated dump
# - repair: install $FROM_VERSION, update to $TO_VERSION and run integrity tests

set -e
set -u

FROM_VERSION=${FROM_VERSION:-$(grep '^downgrade_to_version ' version.config | awk '{ print $3 }')}
TO_VERSION=${TO_VERSION:-$(grep '^version ' version.config | awk '{ print $3 }')}

TEST_REPAIR=${TEST_REPAIR:-false}
TEST_VERSION=${TEST_VERSION:-v8}

OUTPUT_DIR=${OUTPUT_DIR:-update_test/${FROM_VERSION}_to_${TO_VERSION}}
PGDATA="${OUTPUT_DIR}/data"
# Get an unused port to allow	for parallel execution
PGHOST=localhost
PGPORT=${PGPORT:-$(python -c 'import socket; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) ; s.bind(("", 0)); print(s.getsockname()[1]); s.close()')}
PGDATABASE=postgres

export PGHOST PGPORT PGDATABASE PGDATA

run_sql() {
  local db=${2:-$PGDATABASE}
  psql -X -q -d "${db}" -v ON_ERROR_STOP=1 -v VERBOSITY=verbose -v WITH_ROLES=true -v WITH_SUPERUSER=true -v WITH_CHUNK=true -c "${1}"
}

run_sql_file() {
  local db=${2:-$PGDATABASE}
  psql -X -d "${db}" -v ON_ERROR_STOP=1 -v VERBOSITY=verbose -v WITH_ROLES=true -v WITH_SUPERUSER=true -v WITH_CHUNK=true -f "${1}"
}

check_version() {
  psql -X -c "DO \$\$BEGIN PERFORM from pg_available_extension_versions WHERE name='timescaledb' AND version='$1'; IF NOT FOUND THEN RAISE 'Version $1 not available'; END IF; END\$\$;" > /dev/null
}

trap cleanup EXIT

cleanup() {
    # Save status here so that we can return the status of the last
    # command in the script and not the last command of the cleanup
    # function
    local status="$?"
    set +e # do not exit immediately on failure in cleanup handler
    pg_ctl stop > /dev/null
    rm -rf "${PGDATA}"
    exit ${status}
}

mkdir -p "${OUTPUT_DIR}"
rm -rf "${OUTPUT_DIR}/data"
mkdir -p "${OUTPUT_DIR}/data"

UNIX_SOCKET_DIR=$(readlink -f "${OUTPUT_DIR}")

initdb > "${OUTPUT_DIR}/initdb.log" 2>&1
pg_ctl -l "${OUTPUT_DIR}/postgres.log" start -o "-c unix_socket_directories='${UNIX_SOCKET_DIR}' -c timezone=GMT -c client_min_messages=warning -c port=${PGPORT} -c max_prepared_transactions=100 -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off -c max_worker_processes=0"
pg_isready -t 30 > /dev/null

echo -e "\nUpdate test for ${FROM_VERSION} -> ${TO_VERSION}\n"

# caller should ensure that the versions are available
check_version "${FROM_VERSION}"
check_version "${TO_VERSION}"

run_sql_file test/sql/updates/setup.roles.sql > /dev/null

echo "Creating baseline database"
{
  run_sql "CREATE DATABASE baseline;"
  PGDATABASE=baseline
  run_sql "CREATE EXTENSION timescaledb VERSION \"${TO_VERSION}\";"
  run_sql_file test/sql/updates/pre.testing.sql
  run_sql_file test/sql/updates/setup.${TEST_VERSION}.sql
  run_sql "CHECKPOINT;"
  run_sql_file test/sql/updates/setup.check.sql
} > "${OUTPUT_DIR}/baseline.log" 2>&1

echo "Creating updated database"
{
  run_sql "CREATE DATABASE updated;" > "${OUTPUT_DIR}/updated.log"
  PGDATABASE=updated
  run_sql "CREATE EXTENSION timescaledb VERSION \"${FROM_VERSION}\";"
  run_sql_file test/sql/updates/pre.testing.sql
  run_sql_file test/sql/updates/setup.${TEST_VERSION}.sql
  run_sql "CHECKPOINT;" >> "${OUTPUT_DIR}/updated.log"
  run_sql "ALTER EXTENSION timescaledb UPDATE TO \"${TO_VERSION}\";"
  run_sql_file test/sql/updates/setup.check.sql
} > "${OUTPUT_DIR}/updated.log" 2>&1

echo "Creating restored database"
{
  run_sql "CREATE DATABASE restored;"
  PGDATABASE=restored
  run_sql "CREATE EXTENSION timescaledb VERSION \"${TO_VERSION}\";"
  run_sql "ALTER DATABASE restored SET timescaledb.restoring='on';"
  pg_dump -Fc -d updated > "${OUTPUT_DIR}/updated.dump"
  pg_restore -d restored "${OUTPUT_DIR}/updated.dump"
  run_sql "ALTER DATABASE restored RESET timescaledb.restoring;"
} > "${OUTPUT_DIR}/restored.log" 2>&1

run_sql_file test/sql/updates/post.${TEST_VERSION}.sql baseline > "${OUTPUT_DIR}/post.baseline.log"
run_sql_file test/sql/updates/post.${TEST_VERSION}.sql updated > "${OUTPUT_DIR}/post.updated.log"
run_sql_file test/sql/updates/post.${TEST_VERSION}.sql restored > "${OUTPUT_DIR}/post.restored.log"

if [ "${TEST_REPAIR}" = "true" ]; then
  echo "Creating repair database"
  {
    run_sql "CREATE DATABASE repair;"
    PGDATABASE=repair
    run_sql "CREATE EXTENSION timescaledb VERSION \"${FROM_VERSION}\";"
    run_sql_file test/sql/updates/setup.repair.sql baseline
    run_sql "ALTER EXTENSION timescaledb UPDATE TO \"${TO_VERSION}\";"
    run_sql_file test/sql/updates/post.repair.sql baseline
    run_sql_file test/sql/updates/post.integrity_test.sql baseline
  } > "${OUTPUT_DIR}/repair.log" 2>&1
fi

diff -u "${OUTPUT_DIR}/post.baseline.log" "${OUTPUT_DIR}/post.updated.log" | tee "${OUTPUT_DIR}/baseline_vs_updated.diff"
if [ ! -s "${OUTPUT_DIR}/baseline_vs_updated.diff" ]; then
  rm "${OUTPUT_DIR}/baseline_vs_updated.diff"
fi
diff -u "${OUTPUT_DIR}/post.baseline.log" "${OUTPUT_DIR}/post.restored.log" | tee "${OUTPUT_DIR}/baseline_vs_restored.diff"
if [ ! -s "${OUTPUT_DIR}/baseline_vs_restored.diff" ]; then
  rm "${OUTPUT_DIR}/baseline_vs_restored.diff"
fi

if [ -f "${OUTPUT_DIR}/baseline_vs_updated.diff" ] || [ -f "${OUTPUT_DIR}/baseline_vs_restored.diff" ]; then
  echo "Update test for ${FROM_VERSION} -> ${TO_VERSION} failed"
  exit 1
fi


