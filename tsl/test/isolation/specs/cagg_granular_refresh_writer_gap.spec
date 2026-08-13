# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# ===========================================================================
# A writer terminated while it holds a generation pin.
#
# begin_batch increments num_writers for the whole pre-commit drain and
# end_batch decrements it; a flush refuses to drain a generation until that
# count reaches zero.  A backend that exits between the two never decrements,
# and nothing else does it on its behalf: proc_exit releases LWLocks, but
# num_writers is a plain atomic in the tracker.  The flush then waits on a
# writer that no longer exists.
#
# NOTE: pg_terminate_backend kills the session connection, which cannot be
# reconnected between permutations, so the terminated session is used once.
# ===========================================================================

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
    SELECT create_hypertable('conditions', 'time');
    ALTER TABLE conditions SET (
        timescaledb.granular_refresh_column = 'sensor_id',
        timescaledb.granular_refresh_start_offset = '36500 days',
        timescaledb.granular_refresh_end_offset = '1 day'
    );

    CREATE MATERIALIZED VIEW cond_daily
      WITH (timescaledb.continuous) AS
      SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
      FROM conditions
      GROUP BY bucket, sensor_id
      WITH NO DATA;
    ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);

    -- Table to track the victim's PID for termination.
    CREATE TABLE cancelpid (pid INTEGER NOT NULL PRIMARY KEY);

    CREATE OR REPLACE PROCEDURE terminatepids() AS
    $$
    BEGIN
        PERFORM pg_terminate_backend(pid) FROM cancelpid;
        DELETE FROM cancelpid;
    END;
    $$ LANGUAGE plpgsql;
}

teardown
{
    DROP MATERIALIZED VIEW cond_daily;
    DROP TABLE conditions;
    DROP TABLE cancelpid;
}

# Establishes the tracker and flushes a first generation cleanly, so the
# refresh below is draining a generation that a live writer once pinned.
session "P"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "p_insert"  { INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'sensor_a', 1); }
step "p_refresh" { CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10'); }

session "WP"
step "wp_enable"  { SELECT debug_waitpoint_enable('tenant_tracker_in_pin'); }
step "wp_release" { SELECT debug_waitpoint_release('tenant_tracker_in_pin'); }

# The victim: parks inside the pin at pre-commit, then is terminated.  This
# session's connection dies with it and is not reused.
session "V"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "v_register_pid" { INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING; }
step "v_insert" { INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 'sensor_b', 2); }

session "T"
step "t_terminate" { CALL terminatepids(); }

# The refresh has to flip the generation and drain the pinned one.  The timeout
# is what ends the step: without it this waits forever.
session "R"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET statement_timeout = '5s';
}
step "r_refresh" { CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10'); }

# The victim's row never committed, so the hypertable holds only sensor_a --
# yet its abandoned pin is enough to stall the refresh above.
session "V2"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "v2_check" { SELECT sensor_id, count(*) FROM conditions GROUP BY sensor_id ORDER BY sensor_id; }

permutation "p_insert" "p_refresh" "wp_enable" "v_register_pid" "v_insert" "t_terminate" "wp_release" "r_refresh" "v2_check"
