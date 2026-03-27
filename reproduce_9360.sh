#!/usr/bin/env bash
#
# Reproduce GitHub Issue #9360:
#   Continuous aggregate refresh jobs permanently stuck after failover
#   (next_start = -infinity)
#
# This script sets up a 2-node PostgreSQL streaming replication cluster
# with TimescaleDB, triggers a failover while a CAgg refresh is mid-run,
# and checks for the stuck job state.
#
# Requirements:
#   - PostgreSQL + TimescaleDB installed (pg_ctl, psql, pg_basebackup on PATH)
#   - Enough disk space for two data directories
#
# Usage: ./reproduce_9360.sh
#
set -euo pipefail

###############################################################################
# Configuration
###############################################################################
PG_BIN_DIR="$(pg_config --bindir)"
PGCTL="$PG_BIN_DIR/pg_ctl"
PSQL="$PG_BIN_DIR/psql"
PG_BASEBACKUP="$PG_BIN_DIR/pg_basebackup"

BASE_DIR="/tmp/repro_9360"
PRIMARY_DATA="$BASE_DIR/primary"
REPLICA_DATA="$BASE_DIR/replica"
PRIMARY_PORT=15432
REPLICA_PORT=15433
PRIMARY_LOG="$BASE_DIR/primary.log"
REPLICA_LOG="$BASE_DIR/replica.log"
REPL_USER="replicator"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }

cleanup() {
    info "Cleaning up..."
    "$PGCTL" -D "$PRIMARY_DATA" stop -m immediate 2>/dev/null || true
    "$PGCTL" -D "$REPLICA_DATA" stop -m immediate 2>/dev/null || true
    rm -rf "$BASE_DIR"
    info "Cleanup done."
}

###############################################################################
# Step 0: Clean up any previous run
###############################################################################
info "=== Step 0: Cleanup previous runs ==="
"$PGCTL" -D "$PRIMARY_DATA" stop -m immediate 2>/dev/null || true
"$PGCTL" -D "$REPLICA_DATA" stop -m immediate 2>/dev/null || true
rm -rf "$BASE_DIR"
mkdir -p "$BASE_DIR"

###############################################################################
# Step 1: Initialize the primary
###############################################################################
info "=== Step 1: Initialize primary at $PRIMARY_DATA ==="
initdb -D "$PRIMARY_DATA" --no-locale --encoding=UTF8 -A trust > /dev/null

# Configure primary for replication
cat >> "$PRIMARY_DATA/postgresql.conf" <<EOF

# Replication settings
listen_addresses = 'localhost'
port = $PRIMARY_PORT
wal_level = replica
max_wal_senders = 5
hot_standby = on
archive_mode = off

# TimescaleDB
shared_preload_libraries = 'timescaledb'
timescaledb.license = 'timescale'

# Make jobs run frequently so we can catch one mid-run
timescaledb.max_background_workers = 8

# Faster checkpoints so WAL replication is snappier
checkpoint_timeout = 30s
EOF

cat >> "$PRIMARY_DATA/pg_hba.conf" <<EOF
# Allow replication connections from localhost
local   replication     all                                trust
host    replication     all         127.0.0.1/32           trust
host    replication     all         ::1/128                trust
EOF

###############################################################################
# Step 2: Start the primary
###############################################################################
info "=== Step 2: Start primary on port $PRIMARY_PORT ==="
"$PGCTL" -D "$PRIMARY_DATA" -l "$PRIMARY_LOG" -o "-p $PRIMARY_PORT" start -w

# Create a database and install TimescaleDB
createdb -p "$PRIMARY_PORT" repro_db
"$PSQL" -p "$PRIMARY_PORT" -d repro_db -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" > /dev/null

info "Primary is running and TimescaleDB is installed."

###############################################################################
# Step 3: Create hypertable, continuous aggregate, and refresh policy
###############################################################################
info "=== Step 3: Create hypertable + continuous aggregate + policy ==="
"$PSQL" -p "$PRIMARY_PORT" -d repro_db <<'SQL'
-- Source hypertable
CREATE TABLE events (
    time  TIMESTAMPTZ NOT NULL,
    value NUMERIC     NOT NULL
);
SELECT create_hypertable('events', 'time');

-- Continuous aggregate
CREATE MATERIALIZED VIEW events_1m
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 minute', time) AS bucket,
       avg(value) AS avg_value
FROM events
GROUP BY bucket
WITH NO DATA;

-- Refresh policy: runs every 30 seconds for faster reproduction
SELECT add_continuous_aggregate_policy('events_1m',
    start_offset   => INTERVAL '1 hour',
    end_offset     => INTERVAL '1 minute',
    schedule_interval => INTERVAL '30 seconds');
SQL

info "Schema created."

###############################################################################
# Step 4: Insert enough data to make refresh take some time
###############################################################################
info "=== Step 4: Insert data into events table ==="
"$PSQL" -p "$PRIMARY_PORT" -d repro_db <<'SQL'
-- Insert ~500k rows spread over the last hour so the refresh has work to do
INSERT INTO events (time, value)
SELECT now() - (i || ' milliseconds')::interval,
       random() * 100
FROM generate_series(1, 500000) AS i;
SQL

info "Inserted 500k rows."

# ###############################################################################
# # Step 5: Do one manual refresh so the policy has a baseline
# ###############################################################################
# info "=== Step 5: Manual initial refresh ==="
# "$PSQL" -p "$PRIMARY_PORT" -d repro_db -c \
#   "CALL refresh_continuous_aggregate('events_1m', NULL, NULL);"
# info "Initial refresh complete."

###############################################################################
# Step 6: Set up the streaming replica via pg_basebackup
###############################################################################
info "=== Step 6: Create streaming replica at $REPLICA_DATA ==="
"$PG_BASEBACKUP" -h localhost -p "$PRIMARY_PORT" -D "$REPLICA_DATA" \
  -X stream -R -C -S repro_slot -v 2>&1 || true

# Adjust replica port
cat >> "$REPLICA_DATA/postgresql.conf" <<EOF
port = $REPLICA_PORT
EOF

###############################################################################
# Step 7: Start the replica
###############################################################################
info "=== Step 7: Start replica on port $REPLICA_PORT ==="
"$PGCTL" -D "$REPLICA_DATA" -l "$REPLICA_LOG" -o "-p $REPLICA_PORT" start -w

# Verify it's in recovery
IS_REPLICA=$("$PSQL" -p "$REPLICA_PORT" -d repro_db -tAc "SELECT pg_is_in_recovery();")
if [ "$IS_REPLICA" = "t" ]; then
    info "Replica is running and in recovery mode."
else
    err "Replica is NOT in recovery mode. Aborting."
    exit 1
fi

###############################################################################
# Step 8: Make refresh slow, then kill primary while it's running
###############################################################################
info "=== Step 8: Make refresh take longer and catch it mid-run ==="

# Add a bunch of extra continuous aggregates so there's more work to do,
# and use a function to slow down the refresh via pg_sleep inside a trigger.
"$PSQL" -p "$PRIMARY_PORT" -d repro_db <<'SQL'
-- Create a function that slows down inserts into the materialization hypertable
-- by sleeping briefly on each batch. We hook into the cagg's materialization
-- table via a statement-level AFTER trigger.
CREATE OR REPLACE FUNCTION slow_down() RETURNS trigger AS $$
BEGIN
    PERFORM pg_sleep(5);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Find the materialization hypertable for events_1m and add a slow trigger
DO $$
DECLARE
    mat_ht regclass;
BEGIN
    SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass INTO mat_ht
    FROM _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable ht ON ht.id = ca.mat_hypertable_id
    WHERE ca.user_view_name = 'events_1m';

    EXECUTE format('CREATE TRIGGER slow_refresh AFTER INSERT ON %s
                    FOR EACH STATEMENT EXECUTE FUNCTION slow_down()', mat_ht);
END;
$$;
SQL

info "Added slow trigger to materialization table."

# Delete materialized data so the next refresh has to redo everything
"$PSQL" -p "$PRIMARY_PORT" -d repro_db -c \
  "SELECT _timescaledb_functions.invalidate_cagg_refresh_window('events_1m', '-infinity'::timestamptz, 'infinity'::timestamptz);" 2>/dev/null \
  || "$PSQL" -p "$PRIMARY_PORT" -d repro_db -c \
  "DELETE FROM _timescaledb_internal._materialized_hypertable_2;" 2>/dev/null \
  || true

info "Waiting for the refresh job to start running..."

# Tight poll loop — check every 100ms using last_start > last_finish
MAX_WAIT=120
ELAPSED=0
JOB_RUNNING=false

while [ $ELAPSED -lt $MAX_WAIT ]; do
    RUNNING=$("$PSQL" -p "$PRIMARY_PORT" -d repro_db -tAc "
        SELECT count(*) FROM _timescaledb_internal.bgw_job_stat js
        JOIN timescaledb_information.jobs j ON j.job_id = js.job_id
        WHERE j.application_name LIKE 'Refresh Continuous Aggregate%'
          AND js.last_start > js.last_finish;
    " 2>/dev/null || echo "0")

    if [ "$RUNNING" -gt 0 ]; then
        JOB_RUNNING=true
        info "Refresh job is currently running! Killing primary immediately."
        break
    fi

    # Poll quickly (0.2s) to catch the short window
    sleep 0.2
    ELAPSED=$((ELAPSED + 1))
    if [ $((ELAPSED % 25)) -eq 0 ]; then
        SECS=$((ELAPSED / 5))
        info "  ...still waiting (~${SECS} seconds). Checking job status..."
        "$PSQL" -p "$PRIMARY_PORT" -d repro_db -c "
            SELECT js.job_id, js.last_start, js.last_finish, js.next_start
            FROM _timescaledb_internal.bgw_job_stat js
            JOIN timescaledb_information.jobs j ON j.job_id = js.job_id
            WHERE j.application_name LIKE 'Refresh Continuous Aggregate%';
        " 2>/dev/null || true
    fi
done

if [ "$JOB_RUNNING" = false ]; then
    warn "Job did not start within timeout. Killing primary anyway."
fi

###############################################################################
# Step 9: Hard-kill the primary (simulate crash)
###############################################################################
info "=== Step 9: Hard-kill the primary (immediate / SIGKILL) ==="

# Snapshot the job state before kill
info "Job state on primary BEFORE kill:"
"$PSQL" -p "$PRIMARY_PORT" -d repro_db -c "
    SELECT js.job_id, js.last_start, js.last_finish, js.next_start,
           js.consecutive_crashes, js.flags
    FROM _timescaledb_internal.bgw_job_stat js
    JOIN timescaledb_information.jobs j ON j.job_id = js.job_id
    WHERE j.application_name LIKE 'Refresh Continuous Aggregate%';
" 2>/dev/null || true

# Kill the primary with SIGKILL (simulates power loss / OOM kill)
PRIMARY_PID=$(head -1 "$PRIMARY_DATA/postmaster.pid")
info "Sending SIGKILL to primary (PID=$PRIMARY_PID)..."
kill -9 "$PRIMARY_PID" 2>/dev/null || true

# Wait for it to die
sleep 2
if ! kill -0 "$PRIMARY_PID" 2>/dev/null; then
    info "Primary is dead."
else
    err "Primary still alive after SIGKILL!"
    exit 1
fi

###############################################################################
# Step 10: Promote the replica to become the new primary
###############################################################################
info "=== Step 10: Promote replica ==="
"$PGCTL" -D "$REPLICA_DATA" promote -w
sleep 3

IS_PRIMARY=$("$PSQL" -p "$REPLICA_PORT" -d repro_db -tAc "SELECT pg_is_in_recovery();")
if [ "$IS_PRIMARY" = "f" ]; then
    info "Replica has been promoted to primary!"
else
    err "Replica promotion failed."
    exit 1
fi

###############################################################################
# Step 11: Check for the stuck job state
###############################################################################
info "=== Step 11: Check for stuck jobs on new primary (port $REPLICA_PORT) ==="

echo ""
info "Job stats on new primary:"
"$PSQL" -p "$REPLICA_PORT" -d repro_db -c "
    SELECT js.job_id, js.last_start, js.last_finish, js.next_start,
           js.consecutive_crashes, js.flags
    FROM _timescaledb_internal.bgw_job_stat js
    JOIN timescaledb_information.jobs j ON j.job_id = js.job_id
    WHERE j.application_name LIKE 'Refresh Continuous Aggregate%';
"

echo ""
info "Checking for -infinity values (the bug):"
STUCK=$("$PSQL" -p "$REPLICA_PORT" -d repro_db -tAc "
    SELECT count(*) FROM _timescaledb_internal.bgw_job_stat js
    JOIN timescaledb_information.jobs j ON j.job_id = js.job_id
    WHERE j.application_name LIKE 'Refresh Continuous Aggregate%'
      AND (js.last_finish = '-infinity' OR js.next_start = '-infinity');
")

if [ "$STUCK" -gt 0 ]; then
    echo ""
    err "BUG REPRODUCED: Found $STUCK job(s) with -infinity values!"
    err "These jobs will never reschedule after failover."
    echo ""
    info "Detailed view:"
    "$PSQL" -p "$REPLICA_PORT" -d repro_db -c "
        SELECT js.job_id,
               js.last_start,
               js.last_finish,
               js.next_start,
               js.consecutive_crashes,
               js.flags,
               CASE WHEN js.last_finish = '-infinity' THEN 'STUCK (last_finish=-inf)'
                    WHEN js.next_start = '-infinity'  THEN 'STUCK (next_start=-inf)'
                    ELSE 'OK'
               END AS status
        FROM _timescaledb_internal.bgw_job_stat js
        JOIN timescaledb_information.jobs j ON j.job_id = js.job_id
        WHERE j.application_name LIKE 'Refresh Continuous Aggregate%';
    "
else
    warn "No -infinity values found. The job may not have been mid-run during the kill."
    warn "Try running the script again — timing is critical."
    echo ""
    info "Current job state for reference:"
    "$PSQL" -p "$REPLICA_PORT" -d repro_db -c "
        SELECT js.job_id, js.last_start, js.last_finish, js.next_start,
               js.consecutive_crashes, js.flags
        FROM _timescaledb_internal.bgw_job_stat js;
    "
fi

###############################################################################
# Step 12: Wait and check if the job ever reschedules
###############################################################################
echo ""
info "=== Step 12: Waiting 90 seconds to see if the job ever reschedules ==="
for i in $(seq 1 3); do
    sleep 30
    info "Check $i/3 (after $((i * 30)) seconds):"
    "$PSQL" -p "$REPLICA_PORT" -d repro_db -c "
        SELECT js.job_id, js.last_start, js.last_finish, js.next_start,
               js.consecutive_crashes, js.flags
        FROM _timescaledb_internal.bgw_job_stat js
        JOIN timescaledb_information.jobs j ON j.job_id = js.job_id
        WHERE j.application_name LIKE 'Refresh Continuous Aggregate%';
    "
done

###############################################################################
# Step 13: Cleanup
###############################################################################
echo ""
info "=== Done ==="
info "The replica (now primary) is still running on port $REPLICA_PORT."
info "To clean up, run:  $0 --cleanup"
info "Or manually:  $PGCTL -D $REPLICA_DATA stop -m fast && rm -rf $BASE_DIR"

if [ "${1:-}" = "--cleanup" ]; then
    cleanup
fi
