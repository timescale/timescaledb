# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    SELECT _timescaledb_functions.stop_background_workers();
    CREATE TABLE ts_continuous_test(time INTEGER, val INTEGER);
    SELECT create_hypertable('ts_continuous_test', 'time', chunk_time_interval => 10);
    CREATE OR REPLACE FUNCTION integer_now_test() returns INT LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM ts_continuous_test $$;
    SELECT set_integer_now_func('ts_continuous_test', 'integer_now_test');
    INSERT INTO ts_continuous_test SELECT i, i FROM
        (SELECT generate_series(0, 29) AS i) AS i;
}

teardown {
    DROP FUNCTION lock_cagg(integer);
    DROP TABLE ts_continuous_test CASCADE;
}

#needed to avoid "SQL step too long" error in setup
session "SetupContinue"
step "Setup2"
{
    CREATE MATERIALIZED VIEW continuous_view_1( bkt, cnt)
        WITH ( timescaledb.continuous, timescaledb.materialized_only = true)
        AS SELECT time_bucket('5', time), COUNT(val)
            FROM ts_continuous_test
            GROUP BY 1 WITH NO DATA;
    CREATE MATERIALIZED VIEW continuous_view_2(bkt, maxl)
        WITH ( timescaledb.continuous, timescaledb.materialized_only = true)
        AS SELECT time_bucket('5', time), max(val)
            FROM ts_continuous_test
            GROUP BY 1 WITH NO DATA;

    CREATE FUNCTION lock_cagg(mat_hypertable_id integer) RETURNS void AS $$
    BEGIN PERFORM 1 FROM _timescaledb_catalog.continuous_agg ca WHERE ca.mat_hypertable_id = lock_cagg.mat_hypertable_id FOR UPDATE;
    END; $$ LANGUAGE plpgsql;
}

session "I"
step "I1"	{ INSERT INTO ts_continuous_test SELECT 0, i*10 FROM (SELECT generate_series(0, 10) AS i) AS i; }
step "I2"   { INSERT INTO ts_continuous_test SELECT 40, 1000 ; }

session "R1"
setup { SET client_min_messages TO NOTICE; }
step "Refresh1"	{ CALL refresh_continuous_aggregate('continuous_view_1', NULL, 30); }

session "R1_sel"
step "Refresh1_sel"	{ select * from continuous_view_1 where bkt = 0 or bkt > 30 }

session "R2"
setup { SET client_min_messages TO NOTICE; }
step "Refresh2"	{ CALL refresh_continuous_aggregate('continuous_view_2', NULL, NULL); }

session "R2_sel"
step "Refresh2_sel"	{ select * from continuous_view_2 where bkt = 0 or bkt > 30 order by bkt; }

#locking the catalog row will block refresh1
session "LC1"
step "LockCAggCatalogRow_1" { BEGIN; select lock_cagg(mat_hypertable_id) FROM (SELECT mat_hypertable_id from _timescaledb_catalog.continuous_agg where user_view_name like 'continuous_view_1') q; }
step "UnlockCAggCatalogRow_1" { ROLLBACK; }

#update the hypertable
session "Upd"
step "U1" { update ts_continuous_test SET val = 5555 where time < 10; }
step "U2" { update ts_continuous_test SET val = 5 where time > 15 and time < 25; }

#simulate an update to the invalidation threshold table that would lock the hypertable row
#this would block refresh that needs to get a row lock for the hypertable
session "LInv"
step "LInvRow" { BEGIN; update _timescaledb_catalog.continuous_aggs_invalidation_threshold set watermark = 20 where hypertable_id in ( select raw_hypertable_id from _timescaledb_catalog.continuous_agg where user_view_name like 'continuous_view_1' );
}
step "UnlockInvRow" { ROLLBACK; }


# Test 1: Refreshes on different CAggs can run concurrently.
# Refresh1 is initially blocked by the catalog row lock on continuous_view_1. Refresh2 on continuous_view_2 is not blocked and runs concurrently.
permutation "Setup2" "LockCAggCatalogRow_1" "Refresh1" "Refresh2" "UnlockCAggCatalogRow_1"

# Test 2: continuous_view_2 should see results from the insert but not the other one.
# Refresh2 will complete first due to LockCAggCatalogRow_1 and write the invalidation logs out.
# Refresh1 will see these invalidation logs as well
permutation "Setup2" "Refresh1" "Refresh2" "Refresh1_sel" "Refresh2_sel" "LockCAggCatalogRow_1" "I1" "Refresh1" "Refresh2" "Refresh2_sel" "UnlockCAggCatalogRow_1" "Refresh1_sel"

# Test 3: Both see the updates i.e. the invalidations
##Refresh1 and Refresh2 are blocked by LockInvRow, when that is unlocked, they should complete serially
permutation "Setup2" "Refresh1" "Refresh2" "Refresh1_sel" "Refresh2_sel" "U1" "U2" "LInvRow" "Refresh1" "Refresh2" "UnlockInvRow" "Refresh1_sel" "Refresh2_sel"
