# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup
{
    SELECT _timescaledb_internal.stop_background_workers();
    CREATE TABLE ts_continuous_test(time INTEGER, location INTEGER);
    SELECT create_hypertable('ts_continuous_test', 'time', chunk_time_interval => 10);
    CREATE OR REPLACE FUNCTION integer_now_test() returns INT LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM ts_continuous_test $$;
    SELECT set_integer_now_func('ts_continuous_test', 'integer_now_test');
    INSERT INTO ts_continuous_test SELECT i, i FROM
        (SELECT generate_series(0, 29) AS i) AS i;
    CREATE MATERIALIZED VIEW continuous_view
        WITH ( timescaledb.continuous, timescaledb.materialized_only=true)
        AS SELECT time_bucket('5', time), COUNT(location)
            FROM ts_continuous_test
            GROUP BY 1 WITH NO DATA;

    CREATE OR REPLACE FUNCTION lock_cagg(cagg name) RETURNS void AS $$
    DECLARE
      mattable text;
    BEGIN
      SELECT format('%I.%I', user_view_schema, user_view_name)
      FROM _timescaledb_catalog.continuous_agg
      INTO mattable;
      EXECUTE format('LOCK table %s IN EXCLUSIVE MODE', mattable);
    END; $$ LANGUAGE plpgsql;
}

teardown {
    DROP TABLE ts_continuous_test CASCADE;
}

session "I"
step "Ib"	{ BEGIN; SET LOCAL lock_timeout = '500ms'; SET LOCAL deadlock_timeout = '10ms';}
step "I1"	{ INSERT INTO ts_continuous_test VALUES (1, 1); }
step "Ic"	{ COMMIT; }

session "Ip"
step "Ipb"	{ BEGIN; SET LOCAL lock_timeout = '500ms'; SET LOCAL deadlock_timeout = '10ms';}
step "Ip1"	{ INSERT INTO ts_continuous_test VALUES (29, 29); }
step "Ipc"	{ COMMIT; }

session "S"
step "Sb"	{ BEGIN; SET LOCAL lock_timeout = '500ms'; SET LOCAL deadlock_timeout = '10ms';}
step "S1"	{ SELECT count(*) FROM ts_continuous_test; }
step "Sc"	{ COMMIT; }

session "SV"
step "SV1"	{ SELECT * FROM continuous_view order by 1; }

session "R"
setup { SET client_min_messages TO LOG; }
step "Refresh"	{ CALL refresh_continuous_aggregate('continuous_view', NULL, 15); }

session "R1"
setup { SET client_min_messages TO LOG; }
step "Refresh1"	{ CALL refresh_continuous_aggregate('continuous_view', NULL, 15); }

session "R2"
setup { SET lock_timeout = '500ms'; SET deadlock_timeout = '10ms'; }
step "Refresh2"	{ CALL refresh_continuous_aggregate('continuous_view', NULL, 15); }
teardown { SET lock_timeout TO default; SET deadlock_timeout to default; }

# the invalidation log is copied in the first materialization tranasction
session "L"
step "LockInval" { BEGIN; LOCK TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log; }
step "UnlockInval" { ROLLBACK; }

# the invalidation threshold lock will block both INSERT and REFRESH
session "LI"
step "LockInvalThr" { BEGIN; LOCK TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold IN SHARE MODE; }
step "UnlockInvalThr" { ROLLBACK; }

# the invalidation threshold lock will block both INSERT and REFRESH
session "LIE"
step "LockInvalThrEx" { BEGIN; LOCK TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold ; }
step "UnlockInvalThrEx" { ROLLBACK; }

# locking a cagg's materialized hypertable will block the REFRESH in
# the second transaction, but not INSERTs.
session "LC"
step "LockCagg" { BEGIN; SELECT lock_cagg('continuous_view'); }
step "UnlockCagg" { ROLLBACK; }

# the materialization invalidation log
session "LM"
step "LockMatInval" { BEGIN; LOCK TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log; }
step "UnlockMatInval" { ROLLBACK; }
#only one refresh
permutation "LockInvalThrEx" "Refresh2" "Refresh"  "UnlockInvalThrEx"

#refresh and insert do not block each other once refresh is out of the
#first transaction where it moves the invalidation threshold
permutation "Ib" "LockCagg" "I1" "Refresh" "Ic" "UnlockCagg"
permutation "Ib" "LockCagg" "Refresh" "I1" "Ic" "UnlockCagg"

#refresh and select can run concurrently. Refresh blocked only by lock on
# cagg's materialized hypertable. Needs RowExclusive for 2nd txn.
permutation "Sb" "LockCagg" "Refresh" "S1" "Sc" "UnlockCagg"
permutation "Sb" "LockCagg" "S1" "Refresh" "Sc" "UnlockCagg"

#refresh will see new invalidations (you can tell since they are waiting on the invalidation log lock)
permutation "Ib" "LockInvalThr" "Refresh" "I1" "Ic" "UnlockInvalThr"
permutation "Ib" "LockInvalThr" "I1" "Refresh" "Ic" "UnlockInvalThr"

#with no invalidation threshold, inserts will not write to the invalidation log
permutation "Ib" "LockInval" "I1" "Ic" "Refresh" "UnlockInval"

#inserts beyond the invalidation will not write to the log, but since
#the refresh currently takes an exclusive lock on the invalidation
#threshold table (even if not updating the threshold) it will block
#inserts in the first transaction of the refresh
permutation "Ipb" "LockInval" "Refresh" "Ip1" "Ipc" "UnlockInval"
permutation "Ipb" "LockInval" "Ip1" "Refresh" "Ipc" "UnlockInval"
permutation "Ipb" "LockInval" "Ip1" "Ipc" "Refresh" "UnlockInval"


#refresh and insert/select do not block each other
#refresh1 is blocked on LockMatInval , insert is blocked on invalidation threshold. so refresh1 does not see the insert from I1
permutation "Refresh" "SV1" "LockMatInval" "Refresh1" "Ib" "I1" "LockInvalThrEx" "Ic" "UnlockMatInval" "UnlockInvalThrEx" "SV1"
permutation "I1" "Refresh" "LockInval" "Refresh" "Sb" "S1" "Sc" "UnlockInval"
permutation "I1" "Refresh" "LockInval" "Sb" "S1" "Refresh" "Sc" "UnlockInval"
