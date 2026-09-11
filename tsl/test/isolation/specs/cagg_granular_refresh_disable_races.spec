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

# ===========================================================================
# ALTER TABLE <hypertable> SET (timescaledb.cagg_enable_granular_refresh = false)
# against 1) vs DML and 2) vs. cagg-level enable.
#
# The shared setup above leaves cond_daily's own granular refresh enabled, but
# the hypertable-level disable below refuses whenever any of its caggs has
# granular refresh enabled. Every permutation in this section therefore primes
# with "d_disable" to bring the cagg back to disabled before exercising the
# hypertable-level race, matching the state these tests were written against.
# ===========================================================================

# Writes to the hypertable, holding RowExclusiveLock until it commits.
session "HW"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "hw_begin"  { BEGIN; }
step "hw_insert" { INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'sensor_a', 1); }
step "hw_commit" { COMMIT; }

# The hypertable-level disable.
session "HD"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "hd_begin"   { BEGIN; }
step "hd_disable" { ALTER TABLE conditions SET (timescaledb.cagg_enable_granular_refresh = false); }
step "hd_commit"  { COMMIT; }
step "hd_settings" {
    SELECT count(*) AS settings_rows
    FROM _timescaledb_catalog.hypertable_cagg_settings s
    JOIN _timescaledb_catalog.hypertable h ON h.id = s.hypertable_id
    WHERE h.table_name = 'conditions';
}

# The disable inside nested PL/pgSQL EXCEPTION handlers.  Every block with a
# handler runs in an implicit subtransaction.  The inner block runs the DDL and
# exits normally, so its subtransaction commits into the outer one; the outer
# block then raises, so the outer subtransaction rolls the DDL back while the
# surrounding transaction still commits.  The queued shared-memory free was
# recorded under the inner subtransaction, so it has to follow the DDL into the
# outer one on commit and be discarded with it on abort, or the tracker is gone
# while its configuration row is back.
session "HX"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "hx_exception" {
    DO $$
    BEGIN
        BEGIN
            ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = false);
            ALTER TABLE conditions SET (timescaledb.cagg_enable_granular_refresh = false);
        EXCEPTION WHEN others THEN
            RAISE;
        END;
        -- Abort only once the configuration really is cleared, so a disable that
        -- silently did nothing would show up as settings_rows = 0 rather than
        -- letting the permutation pass for the wrong reason.
        IF NOT EXISTS (
            SELECT 1 FROM _timescaledb_catalog.hypertable_cagg_settings s
            JOIN _timescaledb_catalog.hypertable h ON h.id = s.hypertable_id
            WHERE h.table_name = 'conditions') THEN
            RAISE EXCEPTION 'abort the subtransaction';
        END IF;
    EXCEPTION WHEN others THEN
        NULL;
    END $$;
}

# The cagg-level enable, the counterparty for the second lock.
session "HE"
setup { SET timezone TO 'UTC'; SET client_min_messages TO warning; }
step "he_begin"  { BEGIN; }
step "he_enable" { ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true); }
step "he_commit" { COMMIT; }

# Configures granular refresh on the hypertable again after a disable.
session "HC"
setup { SET client_min_messages TO warning; }
step "hc_configure" {
    ALTER TABLE conditions SET (
        timescaledb.granular_refresh_column = 'sensor_id',
        timescaledb.granular_refresh_start_offset = '2 years',
        timescaledb.granular_refresh_end_offset = '1 day');
}

# A writer whose cached tracker handle outlives the tracker.  Mocks now() like
# session P so the 2020 rows fall inside the window it seeds when it creates
# the tracker.
session "S"
setup {
    SET timezone TO 'UTC'; SET client_min_messages TO warning;
    SET timescaledb.current_timestamp_mock = '2021-01-10 00:00:00+00';
}
step "s_insert_a" { INSERT INTO conditions VALUES ('2020-01-02 00:00+00', 'sensor_a', 1); }
step "s_insert_b" { INSERT INTO conditions VALUES ('2020-01-05 00:00+00', 'sensor_b', 2); }

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

# 3. An open writing transaction blocks the hypertable-level disable.  If the
# disable ever stopped taking AccessExclusiveLock this step would run straight
# through, and the shared-memory release it guards would be unsafe.
permutation "d_disable" "hw_begin" "hw_insert" "hd_disable" "hw_commit" "hd_settings"

# 4a. Enable first: the hypertable-level disable waits on the hypertable
# lock, then finds the cagg granular and refuses.  The configuration
# survives.
permutation "d_disable" "he_begin" "he_enable" "hd_disable" "he_commit" "hd_settings" "d_flag"

# 4b. Disable first: the enable waits on the same lock, then finds no
# configuration left and refuses.  The flag stays off.
permutation "d_disable" "hd_begin" "hd_disable" "he_enable" "hd_commit" "hd_settings" "d_flag"

# 5. The disable inside nested PL/pgSQL EXCEPTION handlers.  The inner
# subtransaction commits, the outer one rolls back and the surrounding
# transaction commits, so both the configuration row and cond_daily's own flag
# come back.  The tracker has to come back with them:
# r_refresh flushes the late arrivals in Txn2, and it can only find them if the
# tracker outlived the swallowed exception -- freeing it would leave the flush
# with nothing to drain and r_tracking empty.  No cagg-level priming here, since
# the flush needs cond_daily granular.
permutation "p_prime_insert" "p_prime_refresh" "p_insert_late" "hx_exception" "hd_settings" "d_flag" "r_refresh" "r_tracking"

# 6. A cached tracker handle outlives the tracker.  S writes and caches its
# handle to the tracker; HD frees that tracker at commit; HC and HE configure
# and enable granular refresh again, giving the hypertable a NEW tracker.  S's
# next write must notice the relcache invalidation the disable sent and resolve
# the new tracker instead of writing through the old handle.  r_tracking lists
# only sensor_b: sensor_a sat in the freed tracker, which was never flushed.
# Its invalidation carries a seqnum with no tracking rows, so the refresh falls
# back to a full pass there and r_cagg_contents shows both tenants anyway.
permutation "p_prime_insert" "p_prime_refresh" "s_insert_a" "d_disable" "hd_disable" "hd_settings" "hc_configure" "he_enable" "s_insert_b" "r_refresh" "r_tracking" "r_cagg_contents" "d_flag"
