
setup
{
    SELECT _timescaledb_internal.stop_background_workers();
    CREATE TABLE ts_continuous_test(time INTEGER, val INTEGER);
    SELECT create_hypertable('ts_continuous_test', 'time', chunk_time_interval => 10);
    CREATE OR REPLACE FUNCTION integer_now_test() returns INT LANGUAGE SQL STABLE as $$ SELECT coalesce(max(time), 0) FROM ts_continuous_test $$;
    SELECT set_integer_now_func('ts_continuous_test', 'integer_now_test');
    INSERT INTO ts_continuous_test SELECT i, i FROM
        (SELECT generate_series(0, 29) AS i) AS i;
}

teardown {
    DROP FUNCTION lock_mattable( text );
    DROP TABLE ts_continuous_test CASCADE;
}

#needed to avoid "SQL step too long" error in setup
session "SetupContinue"
step "Setup2"
{
    CREATE VIEW continuous_view_1( bkt, cnt)
        WITH ( timescaledb.continuous, timescaledb.refresh_lag = '-5', timescaledb.refresh_interval='72 hours')
        AS SELECT time_bucket('5', time), COUNT(val)
            FROM ts_continuous_test
            GROUP BY 1;
    CREATE VIEW continuous_view_2(bkt, maxl)
        WITH ( timescaledb.continuous,timescaledb.refresh_lag='-10',  timescaledb.refresh_interval='72 hours')
        AS SELECT time_bucket('5', time), max(val)
            FROM ts_continuous_test
            GROUP BY 1;
    CREATE FUNCTION lock_mattable( name text) RETURNS void AS $$
    BEGIN EXECUTE format( 'lock table %s', name);
    END; $$ LANGUAGE plpgsql;
}

session "I"
step "I1"	{ INSERT INTO ts_continuous_test SELECT 0, i*10 FROM (SELECT generate_series(0, 10) AS i) AS i; }
step "I2"   { INSERT INTO ts_continuous_test SELECT 40, 1000 ; }

step "S1"	{ SELECT count(*) FROM ts_continuous_test; }

session "R1"
setup { SET client_min_messages TO LOG; }
step "Refresh1"	{ REFRESH MATERIALIZED VIEW continuous_view_1; }

session "R1_sel"
step "Refresh1_sel"	{ select * from continuous_view_1 where bkt = 0 or bkt > 30 }

session "R2"
setup { SET client_min_messages TO LOG; }
step "Refresh2"	{ REFRESH MATERIALIZED VIEW continuous_view_2; }

session "R2_sel"
step "Refresh2_sel"	{ select * from continuous_view_2 where bkt = 0 or bkt > 30 order by bkt; }

#the completed threshold will block the REFRESH from writing 
session "LC"
step "LockCompleted" { BEGIN; LOCK TABLE _timescaledb_catalog.continuous_aggs_completed_threshold IN SHARE MODE; }
step "UnlockCompleted" { ROLLBACK; }

#locking the materialized table will block refresh1
session "LM1"
step "LockMat1" { BEGIN; select lock_mattable(materialization_hypertable::text) from timescaledb_information.continuous_aggregates where view_name::text like 'continuous_view_1';
}
step "UnlockMat1" { ROLLBACK; }

#alter the refresh_lag for continuous_view_1
session "CVddl"
step "AlterLag1" { alter view continuous_view_1 set (timescaledb.refresh_lag = 10); }
 
#refresh1, refresh2 can run concurrently
permutation "Setup2" "LockCompleted" "LockMat1" "Refresh1" "Refresh2" "UnlockCompleted" "UnlockMat1"

#refresh1 and refresh2 run concurrently and see the correct invalidation
#test1 - both see the same invalidation
permutation "Setup2" "Refresh1" "Refresh2" "LockCompleted" "LockMat1" "I1" "Refresh1" "Refresh2" "UnlockCompleted" "UnlockMat1" "Refresh1_sel" "Refresh2_sel"
##test2 - continuous_view_2 should see results from insert but not the other one.
## Refresh2 will complete first due to LockMat1 and write the invalidation logs out. 
permutation "Setup2" "AlterLag1" "Refresh1" "Refresh2" "Refresh1_sel" "Refresh2_sel" "LockCompleted" "LockMat1" "I2" "Refresh1" "Refresh2" "UnlockCompleted" "UnlockMat1" "Refresh1_sel" "Refresh2_sel"
