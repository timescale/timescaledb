setup
{
 CREATE TABLE ts_cluster_test(time timestamptz, temp float, location int);
 SELECT schema_name, table_name FROM create_hypertable('ts_cluster_test', 'time', chunk_time_interval => interval '1 day');
}

teardown { DROP TABLE ts_cluster_test; }

session "s1"
setup	    { BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms'; }
step "s1a"	{ INSERT INTO ts_cluster_test VALUES ('2017-01-20T09:00:01', 23.4, 1); }
step "s1c"	{ COMMIT; }

session "s2"
setup	    { BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; SET LOCAL lock_timeout = '50ms'; SET LOCAL deadlock_timeout = '10ms'; }
step "s2a"	{ INSERT INTO ts_cluster_test VALUES ('2017-01-20T09:00:02', 0.72, 1); }
step "s2c"	{ COMMIT; }
