
setup
{
    SELECT _timescaledb_internal.stop_background_workers();
    CREATE TABLE ts_continuous_test(time INTEGER, location INTEGER);
    SELECT create_hypertable('ts_continuous_test', 'time', chunk_time_interval => 10);
    CREATE OR REPLACE FUNCTION integer_now_test() returns INT LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM ts_continuous_test $$;
    SELECT set_integer_now_func('ts_continuous_test', 'integer_now_test');
    INSERT INTO ts_continuous_test SELECT i, i FROM
        (SELECT generate_series(0, 29) AS i) AS i;
    CREATE VIEW continuous_view
        WITH ( timescaledb.continuous, timescaledb.materialized_only=true, timescaledb.refresh_interval='72 hours')
        AS SELECT time_bucket('5', time), COUNT(location)
            FROM ts_continuous_test
            GROUP BY 1;
}

teardown {
    DROP TABLE ts_continuous_test CASCADE;
}

session "I"
step "Ib"	{ BEGIN; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms';}
step "I1"	{ INSERT INTO ts_continuous_test VALUES (1, 1); }
step "Ic"	{ COMMIT; }

session "Ip"
step "Ipb"	{ BEGIN; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms';}
step "Ip1"	{ INSERT INTO ts_continuous_test VALUES (29, 29); }
step "Ipc"	{ COMMIT; }

session "S"
step "Sb"	{ BEGIN; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms';}
step "S1"	{ SELECT count(*) FROM ts_continuous_test; }
step "Sc"	{ COMMIT; }

session "SV"
step "SV1"	{ SELECT * FROM continuous_view order by 1; }

session "R"
setup { SET client_min_messages TO LOG; }
step "Refresh"	{ REFRESH MATERIALIZED VIEW continuous_view; }

session "R1"
setup { SET client_min_messages TO LOG; }
step "Refresh1"	{ REFRESH MATERIALIZED VIEW continuous_view; }

session "R2"
setup { SET lock_timeout = '50ms'; SET deadlock_timeout = '10ms'; }
step "Refresh2"	{ REFRESH MATERIALIZED VIEW continuous_view; }
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

#the completed threshold will block the REFRESH in the second materialization
# txn , but not the INSERT
session "LC"
step "LockCompleted" { BEGIN; LOCK TABLE _timescaledb_catalog.continuous_aggs_completed_threshold; }
step "UnlockCompleted" { ROLLBACK; }

# the materialization invalidation log
session "LM"
step "LockMatInval" { BEGIN; LOCK TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log; }
step "UnlockMatInval" { ROLLBACK; }
#only one refresh
permutation "LockCompleted" "Refresh2" "Refresh"  "UnlockCompleted"

#refresh and insert do not block each other
permutation "Ib" "LockCompleted" "I1" "Refresh" "Ic" "UnlockCompleted"
permutation "Ib" "LockCompleted" "Refresh" "I1" "Ic" "UnlockCompleted"
#refresh and select can run concurrently. refresh blocked only by lock on
# completed. Needs RowExclusive for 2nd txn.
permutation "Sb" "LockCompleted" "Refresh" "S1" "Sc" "UnlockCompleted"
permutation "Sb" "LockCompleted" "S1" "Refresh" "Sc" "UnlockCompleted"

#refresh will see new invalidations (you can tell since they are waiting on the invalidation log lock)
permutation "Ib" "LockInvalThr" "Refresh" "I1" "Ic" "UnlockInvalThr"
permutation "Ib" "LockInvalThr" "I1" "Refresh" "Ic" "UnlockInvalThr"

#with no invalidation threshold, inserts will not write to the invalidation log
permutation "Ib" "LockInval" "I1" "Ic" "Refresh" "UnlockInval"

#inserts beyond the invalidation will not write to the log
permutation "Ipb" "LockInval" "Refresh" "Ip1" "Ipc" "UnlockInval"
permutation "Ipb" "LockInval" "Ip1" "Refresh" "Ipc" "UnlockInval"
permutation "Ipb" "LockInval" "Ip1" "Ipc" "Refresh" "UnlockInval"


#refresh and insert/select do not block each other
#refresh1 is blocked on LockMatInval , insert is blocked on invalidation threshold. so refresh1 does not see the insert from I1
permutation "Refresh" "SV1" "LockMatInval" "Refresh1" "Ib" "I1" "LockInvalThrEx" "Ic" "UnlockMatInval" "UnlockInvalThrEx" "SV1"
permutation "I1" "Refresh" "LockInval" "Refresh" "Sb" "S1" "Sc" "UnlockInval"
permutation "I1" "Refresh" "LockInval" "Sb" "S1" "Refresh" "Sc" "UnlockInval"
