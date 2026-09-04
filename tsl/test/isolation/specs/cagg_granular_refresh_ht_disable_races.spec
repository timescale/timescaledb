# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# ===========================================================================
# ALTER TABLE <hypertable> SET (timescaledb.enable_cagg_granular_refresh = false)
# against the two things it has to be serialized with.
#
# ALTER TABLE carrying only timescaledb options never reaches
# standard_ProcessUtility, so PostgreSQL takes no relation lock for it at all.
# The disable takes AccessExclusiveLock on the raw hypertable itself, and the
# cagg-level enable/disable takes ShareUpdateExclusiveLock on the same relation.
# Everything below is about those two locks:
#
#   1. vs DML.  The disable must wait for an open transaction that has written
#      to the hypertable.  That is what makes releasing the tenant tracker's
#      shared memory safe: the write path drains into the tracker at
#      XACT_EVENT_PRE_COMMIT, before the writer's locks are released, so no
#      writer can be in flight once the lock is held.
#
#   2. vs the cagg-level enable.  Without a common lock, "no cagg has granular
#      refresh enabled" and the enable itself could interleave, leaving an
#      enabled cagg with no hypertable configuration.  Whichever of the two
#      commits second must fail.
#
# Both waits are on heavyweight locks, which isolationtester detects on its own,
# so no markers or synchronization steps are needed here.
# ===========================================================================

setup
{
    SELECT _timescaledb_functions.stop_background_workers();

    CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
    SELECT create_hypertable('conditions', 'time');
    ALTER TABLE conditions SET (
        timescaledb.granular_refresh_column = 'sensor_id',
        timescaledb.granular_refresh_start_offset = '2 years',
        timescaledb.granular_refresh_end_offset = '1 day'
    );

    CREATE MATERIALIZED VIEW cond_daily
      WITH (timescaledb.continuous) AS
      SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
      FROM conditions
      GROUP BY bucket, sensor_id
      WITH NO DATA;
}

teardown
{
    DROP MATERIALIZED VIEW cond_daily;
    DROP TABLE conditions;
}

# Writes to the hypertable, holding RowExclusiveLock until it commits.
session "W"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "w_begin"  { BEGIN; }
step "w_insert" { INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'sensor_a', 1); }
step "w_commit" { COMMIT; }

# The hypertable-level disable.
session "D"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "d_begin"   { BEGIN; }
step "d_disable" { ALTER TABLE conditions SET (timescaledb.enable_cagg_granular_refresh = false); }
step "d_commit"  { COMMIT; }
step "d_settings" {
    SELECT count(*) AS settings_rows
    FROM _timescaledb_catalog.hypertable_cagg_settings s
    JOIN _timescaledb_catalog.hypertable h ON h.id = s.hypertable_id
    WHERE h.table_name = 'conditions';
}

# The cagg-level enable, the counterparty for the second lock.
session "E"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "e_begin"  { BEGIN; }
step "e_enable" { ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true); }
step "e_commit" { COMMIT; }
step "e_flag" {
    SELECT granular_refresh_enabled FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'cond_daily';
}

# 1. An open writing transaction blocks the disable.  If the disable ever
# stopped taking AccessExclusiveLock this step would run straight through, and
# the shared-memory release it guards would be unsafe.
permutation "w_begin" "w_insert" "d_disable" "w_commit" "d_settings"

# 2a. Enable first: the disable waits on the hypertable lock, then finds the
# cagg granular and refuses.  The configuration survives.
permutation "e_begin" "e_enable" "d_disable" "e_commit" "d_settings" "e_flag"

# 2b. Disable first: the enable waits on the same lock, then finds no
# configuration left and refuses.  The flag stays off.
permutation "d_begin" "d_disable" "e_enable" "d_commit" "d_settings" "e_flag"
