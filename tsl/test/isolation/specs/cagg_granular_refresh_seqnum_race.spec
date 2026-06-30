# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# ===========================================================================
# Granular refresh with a concurrent writer.
# ===========================================================================
#
# Checks that a tenant inserted while a refresh is running still gets
# materialized.
#
# The race: a refresh picks WHICH buckets to rebuild from the invalidation log
# (copied in txn1), but picks WHICH tenants to rebuild from the tracker hints
# (drained later, in txn2/txn3).  A writer that commits in between falls in the
# gap: its bucket isn't in the log copy yet, but its hint is already drained and
# consumed.  Without seqnum correlation the next refresh would see other
# tenants' hints, run granular, skip this one, and leave its bucket stale.
# The fix stamps log entries with the tracker seqnum, so an invalidation without
# associated trackers falls back to the full log instead of skipping.
# Both permutations materialize sensor_b in v_refresh; v_force just re-checks.
#
# Setup: prime the invalidation threshold above the 2020 data so later inserts
# make narrow per-bucket invalidations (a first refresh of a WITH NO DATA cagg
# would refresh the whole window).  Tenant tracking only applies to late data,
# so everything uses 2020 timestamps.
#
#   sensor_a @ 2020-01-01 -- control, refreshed normally by R.
#   sensor_b @ 2020-01-02 -- inserted while R is mid-refresh; must still show up.
#   sensor_c @ 2020-01-03 -- present so the later refresh V runs granular.
#
# Permutation 1: sensor_b commits before R's flush.  Permutation 2: sensor_b
# parks just before commit, so R drains its hint before the row is visible.

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
}

teardown
{
    DROP MATERIALIZED VIEW cond_daily;
    DROP TABLE conditions;
}

# Priming: move the invalidation threshold above the 2020-01 test data so later
# inserts are late-arriving and produce narrow per-bucket invalidations.
session "P"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "p_prime_insert"  { INSERT INTO conditions VALUES ('2020-03-01 00:00+00', 'sensor_prime', 9); }
step "p_prime_refresh" { CALL refresh_continuous_aggregate('cond_daily', '2019-01-01', '2021-01-01', options => jsonb_build_object('buckets_per_batch', 0)); }

# The control tenant, inserted before R starts.
session "S1"
setup { SET client_min_messages TO warning; }
step "s1_insert_a" { INSERT INTO conditions VALUES ('2020-01-01 00:00+00', 'sensor_a', 1); }

# Waitpoint control.
#   wp_*     : park R after txn1 (log copy committed) and before txn2 (tenant
#              flush).  buckets_per_batch=0 -> single, non-batched pass, so the
#              psprintf'd waitpoint name resolves with processing_batch = 1.
#   wp_pc_*  : park a writer in its xact PRE_COMMIT, AFTER its tenant hint is
#              published to shared memory (pin released) but BEFORE its commit
#              record -- so a refresh's flush drains the hint while the writer's
#              invalidation-log entry is still invisible (permutation 2).
session "WP"
step "wp_enable"  { SELECT debug_waitpoint_enable('cagg_policy_batch_1_after_txn_1_wait'); }
step "wp_release" { SELECT debug_waitpoint_release('cagg_policy_batch_1_after_txn_1_wait'); }
step "wp_pc_enable"  { SELECT debug_waitpoint_enable('tenant_tracker_after_precommit_drain'); }
step "wp_pc_release" { SELECT debug_waitpoint_release('tenant_tracker_after_precommit_drain'); }

# The racy refresh: copies sensor_a's log entry in txn1, then parks.
session "R"
setup { SET client_min_messages TO warning; }
step "r_refresh" { CALL refresh_continuous_aggregate('cond_daily', '2019-01-01', '2021-01-01', options => jsonb_build_object('buckets_per_batch', 0)); }

# The VICTIM writer: commits while R is parked between txn1 and txn2.
session "S2"
setup { SET client_min_messages TO warning; }
step "s2_insert_b" { INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'sensor_b', 2); }

# The "other tenant" writer: present at V time so V runs granular.
session "S3"
setup { SET client_min_messages TO warning; }
step "s3_insert_c" { INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 'sensor_c', 3); }

# Verification + recovery.
session "V"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "v_refresh" { CALL refresh_continuous_aggregate('cond_daily', '2019-01-01', '2021-01-01', options => jsonb_build_object('buckets_per_batch', 0)); }
# sensor_b's 2020-01-02 bucket is materialized here.
step "v_check_cagg"
{
    SELECT sensor_id, bucket, avg FROM cond_daily ORDER BY sensor_id, bucket;
}
# Ground truth from the hypertable, to compare against the cagg above.
step "v_check_truth"
{
    SELECT sensor_id, time_bucket('1 day', time) AS bucket, avg(value)
    FROM conditions GROUP BY sensor_id, bucket ORDER BY sensor_id, bucket;
}
# A force refresh recomputes every bucket; the cagg already matches, so this
# just re-checks.
step "v_force"   { CALL refresh_continuous_aggregate('cond_daily', '2019-01-01', '2021-01-01', force => true, options => jsonb_build_object('buckets_per_batch', 0)); }
step "v_recheck"
{
    SELECT sensor_id, bucket, avg FROM cond_daily ORDER BY sensor_id, bucket;
}

# Permutation 1 -- victim commits BEFORE the flush (invisible to R's txn1 snapshot).
permutation "p_prime_insert" "p_prime_refresh" "s1_insert_a" "wp_enable" "r_refresh" "s2_insert_b" "wp_release" "s3_insert_c" "v_refresh" "v_check_cagg" "v_check_truth" "v_force" "v_recheck"

# Permutation 2 -- victim commits AFTER the flush.  s2_insert_b parks in its own
# PRE_COMMIT (hint published, not yet committed); R runs to completion, draining
# and consuming the hint; only then is s2_insert_b released to commit.
permutation "p_prime_insert" "p_prime_refresh" "s1_insert_a" "wp_pc_enable" "s2_insert_b" "r_refresh" "wp_pc_release" "s3_insert_c" "v_refresh" "v_check_cagg" "v_check_truth" "v_force" "v_recheck"
