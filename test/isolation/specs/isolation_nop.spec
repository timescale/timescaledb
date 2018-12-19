setup{
    CREATE TABLE ts_cluster_test(time timestamptz, temp float, location int);
    SELECT table_name from create_hypertable('ts_cluster_test', 'time', chunk_time_interval => interval '1 day');
}

teardown {
    DROP TABLE ts_cluster_test;
}

session "s1"
step "s1a"	{ SELECT pg_sleep(0); }
