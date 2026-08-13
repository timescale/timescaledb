# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# ===========================================================================
# A writer cancelled or terminated while it holds a generation pin.
#
# begin_batch increments num_writers for the whole pre-commit drain and
# end_batch decrements it; a flush refuses to drain a generation until that
# count reaches zero.  A backend that exits between the two never decrements it
# itself, so the abort handler does: a cancel unwinds through the ordinary
# abort, and a terminate reaches the same handler because ShutdownPostgres
# aborts the open transaction before shared memory is detached.  Either way the
# refresh below drains the generation instead of waiting on a writer that is
# gone.
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

    CREATE OR REPLACE PROCEDURE cancelpids() AS
    $$
    BEGIN
        PERFORM pg_cancel_backend(pid) FROM cancelpid;
        DELETE FROM cancelpid;
    END;
    $$ LANGUAGE plpgsql;

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
# Recorded into the generation the victim is about to pin, so the checks at the
# end can show it was still flushed correctly.
step "p_insert2" { INSERT INTO conditions VALUES ('2020-01-04 00:00+00', 'sensor_c', 3); }

session "WP"
step "wp_enable"  { SELECT debug_waitpoint_enable('tenant_tracker_in_pin'); }
step "wp_release" { SELECT debug_waitpoint_release('tenant_tracker_in_pin'); }

# Cancelled victim: parks inside the pin at pre-commit and is cancelled, so the
# pin unwinds through ERROR.  The connection survives, so this session is reused.
session "V"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "v_register_pid" { INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING; }
step "v_insert" { INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 'sensor_b', 2); }

session "K"
step "k_cancel" { CALL cancelpids(); }
# Empty step: gives the victim's unwind somewhere to be observed before
# wp_release runs, so the "<... completed>" line lands deterministically.
step "k_noop" { }

# Terminated victim: same park, but the backend exits.  pg_terminate_backend
# kills the session connection, which cannot be reconnected between
# permutations, so this session is used by the last permutation only.
session "VK"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "vk_register_pid" { INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING; }
step "vk_insert" { INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 'sensor_b', 2); }

session "T"
step "t_terminate" { CALL terminatepids(); }
step "t_noop" { }

# The refresh has to flip the generation and drain the pinned one, which it can
# only do because the abort released the pin.  The timeout is a ceiling so that
# a regression fails here instead of hanging the tester; it is not the assertion,
# so it is set well above anything a loaded runner needs.
session "R"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET statement_timeout = '60s';
}
step "r_refresh" { CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-10'); }

# The victim's row never committed, so the hypertable holds only the rows
# written around it, and the refresh above went through rather than stalling on
# the pin the victim left behind.
session "V2"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "v2_check" { SELECT sensor_id, count(*) FROM conditions GROUP BY sensor_id ORDER BY sensor_id; }
# The generation was drained, so the tenant written before the victim is
# persisted, and the victim's own tenant never appears.
step "v2_tracking"
{
    SELECT tenant_id, seqnum
    FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
    WHERE hypertable_id = (
        SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_name = 'cond_daily')
    ORDER BY seqnum, tenant_id;
}

# Cancel: the writer unwinds via ERROR while holding the pin.
permutation "p_insert" "p_refresh" "p_insert2" "wp_enable" "v_register_pid" "v_insert"("wp_enable") "k_cancel"("v_insert") "k_noop" "wp_release" "r_refresh" "v2_check" "v2_tracking"

# Terminate: the writer exits without unwinding.  Must be last -- session VK's
# connection does not survive it.
permutation "p_insert" "p_refresh" "p_insert2" "wp_enable" "vk_register_pid" "vk_insert"("wp_enable") "t_terminate"("vk_insert") "t_noop" "wp_release" "r_refresh" "v2_check" "v2_tracking"
