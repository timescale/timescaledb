# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# ===========================================================================
# an aborted transaction can leave a tenant info behind in the tracker.a
# This is benign as cagg refresh for a tenant is an idempotent operation
# ===========================================================================

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
    SELECT create_hypertable('conditions', 'time');
    ALTER TABLE conditions SET (
          timescaledb.granular_refresh_column = 'sensor_id',
          timescaledb.granular_refresh_start_offset = '10 years',
          timescaledb.granular_refresh_end_offset = '1 day'
    );

    CREATE MATERIALIZED VIEW cond_daily
      WITH (timescaledb.continuous) AS
      SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
      FROM conditions
      GROUP BY bucket, sensor_id
      WITH NO DATA;
    ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);
    INSERT INTO conditions VALUES ('2026-06-20 00:00+00', 'sensor_1', 1); 

    -- Table to track the victim's PID for cancellation.
    CREATE TABLE cancelpid (pid INTEGER NOT NULL PRIMARY KEY);

    -- Signal cancel to registered backends.  
    CREATE OR REPLACE PROCEDURE cancelpids() AS
    $$
    BEGIN
        PERFORM pg_cancel_backend(pid) FROM cancelpid;
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

# Park a writer in its own PRE_COMMIT: tenant is already published to shared
# memory
session "WP"
step "wp_pc_enable"  { SELECT debug_waitpoint_enable('tenant_tracker_after_precommit_drain'); }
step "wp_pc_release" { SELECT debug_waitpoint_release('tenant_tracker_after_precommit_drain'); }

# victim that will be cancelled: its INSERT will publish a tenant, park at PRE_COMMIT, then
# get cancelled -- aborting the transaction AFTER the tenant was published.
session "V"
setup { SET client_min_messages TO warning; }
step "v_register_pid" { INSERT INTO cancelpid VALUES (pg_backend_pid()) ON CONFLICT (pid) DO NOTHING; }
step "v_insert" { INSERT INTO conditions VALUES ('2026-07-01 00:00+00', 'sensor_victim', 1); }

# Cancels while it is parked at the waitpoint.
session "K"
step "k_cancel" { CALL cancelpids(); }

# An unrelated writer + refresh flushes all tenant tracking entries
# The refresh window excludes the cancelled backend's  
# data, so the flushed tenant is not consumed and stays inspectable.
session "R"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "r_anchor_insert" { INSERT INTO conditions VALUES ('2026-07-15 00:00+00', 'sensor_anchor', 0); }
step "r_refresh" { CALL refresh_continuous_aggregate('cond_daily', '2026-07-15 00:00+00', NULL); }

# Verification.
# no data for sensor_victim. It was rolled back
step "r_check_conditions"
{
    SELECT count(*) AS victim_rows_in_hypertable
    FROM conditions
    WHERE sensor_id = 'sensor_victim';
}
#the victim's tenant is present in tenant tracker
step "r_check_tracking"
{
    SELECT tenant_id, seqnum
    FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
    WHERE hypertable_id = (
        SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_name = 'cond_daily')
    ORDER BY tenant_id, seqnum;
}

## refresh works correctly even though we have a victim tenant.
step "r_refresh2" { CALL refresh_continuous_aggregate('cond_daily', '2026-06-15 00:00+00', NULL) ; } 
step "r_check_cagg" { SELECT * FROM cond_daily ORDER BY 1, 2; }
##orphaned tenant needs to be cleaned up. Need a fix for this
step "r_check_tracking2"
{
    SELECT tenant_id, seqnum
    FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
    WHERE hypertable_id = (
        SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_name = 'cond_daily')
    ORDER BY tenant_id, seqnum;
}

permutation "wp_pc_enable" "v_register_pid" "v_insert"("wp_pc_enable") "k_cancel"("v_insert") "wp_pc_release" "r_anchor_insert"("wp_pc_release") "r_refresh" "r_check_conditions" "r_check_tracking" "r_refresh2" "r_check_cagg" "r_check_tracking2"
