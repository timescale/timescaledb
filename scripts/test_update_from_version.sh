#!/usr/bin/env bash

# During the update test the following databases will be created:
# - baseline: fresh installation of $TO_VERSION
# - updated: install $FROM_VERSION, update to $TO_VERSION
# - restored: restore from updated dump
#
# UPDATE_MODE controls how the "updated" database is updated:
# - direct (default): jump straight from $FROM_VERSION to $TO_VERSION
# - singlestep: update through every intermediate version one at a time

set -xeu

FROM_VERSION=${FROM_VERSION:-$(grep '^previous_version ' version.config | awk '{ print $3 }')}
TO_VERSION=${TO_VERSION:-$(grep '^version ' version.config | awk '{ print $3 }')}

TEST_VERSION=${TEST_VERSION:-v10}
UPDATE_MODE=${UPDATE_MODE:-direct}

OUTPUT_DIR=${OUTPUT_DIR:-update_test/${FROM_VERSION}_to_${TO_VERSION}_${UPDATE_MODE}}
PGDATA="${OUTPUT_DIR}/data"
# Get an unused port to allow	for parallel execution
PGHOST=localhost
PGPORT=${PGPORT:-$(python -c 'import socket; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) ; s.bind(("", 0)); print(s.getsockname()[1]); s.close()')}
PGDATABASE=postgres

export PGHOST PGPORT PGDATABASE PGDATA

run_sql() {
  local db=${2:-$PGDATABASE}
  psql -X -q --echo-queries -d "${db}" -v ON_ERROR_STOP=1 -v VERBOSITY=verbose -v WITH_ROLES=true -v WITH_SUPERUSER=true -v WITH_CHUNK=true -c "${1}"
}

run_sql_file() {
  local db=${2:-$PGDATABASE}
  psql -X -q --echo-queries -d "${db}" -v ON_ERROR_STOP=1 -v VERBOSITY=verbose -v WITH_ROLES=true -v WITH_SUPERUSER=true -v WITH_CHUNK=true -f "${1}"
}

check_version() {
  psql -X -c "DO \$\$BEGIN PERFORM from pg_available_extension_versions WHERE name='timescaledb' AND version='$1'; IF NOT FOUND THEN RAISE 'Version $1 not available'; END IF; END\$\$;" > /dev/null
}

# Single-step transitions that are known to be broken when applied on their own
# (e.g. an old release script that predates a later fix).
SINGLESTEP_SKIP=(
  # 2.10.2 rebuilds bgw_job and casts the owner column to regrole. The cast was
  # unquoted (owner::regrole) and fails on owners that need quoting; it was fixed
  # in 2.11.2.
  "2.10.1--2.10.2"
  "2.10.1--2.10.3"
  "2.10.1--2.11.0"
  "2.10.1--2.11.1"
  # The 2.14.0 update script fails on tables with dropped columns; this was
  # fixed in 2.15.0, so skip the broken edges and step straight to 2.15.0.
  "2.13.1--2.14.0"
  "2.13.1--2.14.1"
  "2.13.1--2.14.2"
)

# Updates starting before 2.15 carry a fixture that the 2.28.0 and 2.28.1 update
# scripts can't migrate (int->bigint view change); the 2.28.2 update script handles
# it, so step from 2.27.2 straight to 2.28.2 when updating from <2.15.
if [ "$(echo "${FROM_VERSION}" | awk -F. '{print $2}')" -lt 15 ]; then
  SINGLESTEP_SKIP+=("2.27.2--2.28.0" "2.27.2--2.28.1")
fi

# Update the current database one version at a time from FROM_VERSION to
# TO_VERSION, exercising every intermediate update script. We can't step through
# the versions in numeric order because back-patch releases (e.g. 2.21.4 was
# released after 2.22.0) have no update script to the next numeric version.
# Instead we repeatedly move to the lowest version reachable from the current
# one through a single update script, ignoring transitions listed in
# SINGLESTEP_SKIP.
singlestep_update() {
  local current="${FROM_VERSION}" next skip_clause=""
  if [ ${#SINGLESTEP_SKIP[@]} -gt 0 ]; then
    local quoted
    quoted=$(printf "'%s'," "${SINGLESTEP_SKIP[@]}")
    skip_clause="AND source || '--' || target NOT IN (${quoted%,})"
  fi
  while [ "${current}" != "${TO_VERSION}" ]; do
    next=$(psql -X -At -d "${PGDATABASE}" -c "
      SELECT target FROM pg_extension_update_paths('timescaledb')
      WHERE source = '${current}' AND path = source || '--' || target
        ${skip_clause}
        AND string_to_array(regexp_replace(target, '[^0-9.].*\$', ''), '.')::int[]
            > string_to_array(regexp_replace('${current}', '[^0-9.].*\$', ''), '.')::int[]
      ORDER BY string_to_array(regexp_replace(target, '[^0-9.].*\$', ''), '.')::int[]
      LIMIT 1")
    run_sql "ALTER EXTENSION timescaledb UPDATE TO \"${next}\";"
    current="${next}"
  done
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

# Don't try to wrap the settings, pg_ctl can't handle newlines there.
pg_ctl -l "${OUTPUT_DIR}/postgres.log" start -o "-c unix_socket_directories=${UNIX_SOCKET_DIR} -c timezone=GMT -c client_min_messages=warning -c port=${PGPORT} -c max_prepared_transactions=100 -c shared_preload_libraries=timescaledb -c timescaledb.telemetry_level=off -c timescaledb.enable_compression_ratio_warnings=off -c max_worker_processes=0 -c log_statement=all"

pg_isready -t 30 > /dev/null

echo -e "\nUpdate test version ${TEST_VERSION} (${UPDATE_MODE}) for ${FROM_VERSION} -> ${TO_VERSION}\n"

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
  if [ "${UPDATE_MODE}" = singlestep ]; then
    singlestep_update
  else
    run_sql "ALTER EXTENSION timescaledb UPDATE TO \"${TO_VERSION}\";"
  fi
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

sed -i -E -e 's!"*_ts_meta_v2_bl[0-9A-Za-z]+_([_0-9A-Za-z]+)"*!regress-test-bloom_\1!g' \
    "${OUTPUT_DIR}"/post.*.log

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
