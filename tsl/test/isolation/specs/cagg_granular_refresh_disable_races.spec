# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# ===========================================================================
# ALTER MATERIALIZED VIEW ... SET (timescaledb.enable_granular_refresh = false)
# against a refresh of the same cagg.
#
# Both sides take LockTupleExclusive on the cagg's _timescaledb_catalog.
# continuous_agg row: the refresh in Txn2 and the DDL. The row lock is the
# coordination mechanism
#
# What each permutation asserts is whether Txn2 ran the granular path, read off
# the tenant-tracking catalog: Txn2 flushes the tracker only when the cagg it
# re-read under the row lock still has granular refresh enabled, so rows for the
# late-arriving tenants appear if and only if the granular path ran.
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
    ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);

    CREATE FUNCTION lock_cagg(mat_hypertable_id integer) RETURNS void AS $$
    BEGIN PERFORM 1 FROM _timescaledb_catalog.continuous_agg ca WHERE ca.mat_hypertable_id = lock_cagg.mat_hypertable_id FOR UPDATE;
    END; $$ LANGUAGE plpgsql;
}

teardown
{
    DROP FUNCTION lock_cagg(integer);
    DROP MATERIALIZED VIEW cond_daily;
    DROP TABLE conditions;
}

# Priming: move the invalidation threshold above the test data so the later
# inserts are late-arriving, then write the tenants the refresh will flush.
session "P"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET timescaledb.current_timestamp_mock = '2021-01-10 00:00:00+00';
}
step "p_prime_insert" {
    INSERT INTO conditions VALUES ('2020-06-01 00:00+00', 'sensor_prime', 9);
}
step "p_prime_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2019-01-01', '2021-01-01', options => jsonb_build_object('buckets_per_batch', 0));
}
step "p_insert_late" {
    INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'sensor_a', 1),
                                  ('2020-01-05 00:00+00', 'sensor_b', 2);
}

# Holds the cagg catalog row lock that refresh Txn2 and the disable DDL both
# want.  ROLLBACK releases it without altering the row.
session "L"
setup { SET client_min_messages TO warning; }
step "l_lock" {
    BEGIN;
    SELECT lock_cagg(mat_hypertable_id) FROM (
        SELECT mat_hypertable_id FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_name = 'cond_daily') q;
}
step "l_unlock" { ROLLBACK; }

session "R"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
}
step "r_refresh" {
    CALL refresh_continuous_aggregate('cond_daily', '2020-01-01', '2020-01-11', options => jsonb_build_object('buckets_per_batch', 0));
}
# Did Txn2 flush the tracker for the late arrivals?  Both rows means the granular
# path ran, no rows means it fell back to the plain path.  Deliberately in
# session R: a step cannot launch until every earlier step of its own session is
# reported complete, so this is guaranteed to run after r_refresh has finished
# both Txn2 and Txn3.  Txn3 no longer reclaims trackings (the garbage collector
# does, and it keeps the recent seqnums), so the flushed rows are still here.
step "r_tracking" {
    SELECT tt.tenant_id
    FROM _timescaledb_catalog.continuous_aggs_tenant_tracking tt
    WHERE tt.hypertable_id = (SELECT raw_hypertable_id
                              FROM _timescaledb_catalog.continuous_agg
                              WHERE user_view_name = 'cond_daily')
      AND tt.tenant_id IN ('sensor_a', 'sensor_b')
    ORDER BY tt.tenant_id;
}
# What the refresh actually materialized.  In session R for the same ordering
# reason as r_tracking.  Used only by the last permutation, where the DDL lands
# between Txn2 and Txn3 and it is worth showing that Txn3 still produced the
# right rows: both late arrivals materialized, and the priming bucket untouched.
step "r_cagg_contents" {
    SELECT bucket, sensor_id, avg FROM cond_daily ORDER BY bucket, sensor_id;
}

# Parks the refresh at the top of Txn3, i.e. after Txn2 has committed and
# released the cagg row lock but before any materialization has happened.  The
# waitpoint is an advisory lock, so isolationtester sees the wait in pg_locks
# and reports "<waiting ...>" on its own -- no blocker marker needed.
session "W"
step "w_before_txn3_enable"  { SELECT debug_waitpoint_enable('after_process_cagg_invalidations_for_refresh_lock'); }
step "w_before_txn3_release" { SELECT debug_waitpoint_release('after_process_cagg_invalidations_for_refresh_lock'); }

session "D"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "d_disable" {
    ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = false);
}
step "d_flag" {
    SELECT granular_refresh_enabled FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'cond_daily';
}

# The refresh alone: Txn2 must block on the row lock.  If it ever stopped taking
# the lock this step would run straight through.
permutation "p_prime_insert" "p_prime_refresh" "p_insert_late" "l_lock" "r_refresh" "l_unlock"

# The DDL alone: it must block on the same row lock.
permutation "p_prime_insert" "p_prime_refresh" "p_insert_late" "l_lock" "d_disable" "l_unlock"

# Both queued, DDL first.  On release the DDL commits, then Txn2 takes the lock,
# re-reads the cagg and finds granular refresh off, so it skips the flush and the
# whole refresh runs the plain path.  r_refresh cannot acquire the lock until
# d_disable has committed, so the completion order is causally fixed.
# refresh sees disabled, so NO tracking entries are flushed by refresh
permutation "p_prime_insert" "p_prime_refresh" "p_insert_late" "l_lock" "d_disable" "r_refresh"("d_disable") "l_unlock" "r_tracking" "d_flag"

# Refresh first, DDL squeezed into the gap between Txn2 and Txn3.  The waitpoint
# pins that gap: r_refresh parks at the top of Txn3, which isolationtester can
# only observe once Txn2 has committed and dropped the cagg row lock, so
# d_disable then launches, takes the row lock uncontended and commits before the
# waitpoint is released and Txn3 resumes.
#
# Txn2 saw granular refresh still enabled, so it flushed the tracker: r_tracking
# returns both late-arriving tenants.  Txn3 keeps using the cagg tuple Txn2
# re-read under the row lock, so it materializes on the granular path even
# though the catalog now says disabled (d_flag is false).  That is safe: the
# tracking rows the refresh consults were committed by Txn2
permutation "p_prime_insert" "p_prime_refresh" "p_insert_late" "w_before_txn3_enable" "r_refresh" "d_disable" "w_before_txn3_release" "r_tracking" "r_cagg_contents" "d_flag"
