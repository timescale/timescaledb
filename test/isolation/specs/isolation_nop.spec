# This file and its contents are licensed under the Apache License 2.0.
# Please see the included NOTICE for copyright information and
# LICENSE-APACHE for a copy of the license.

setup{
    CREATE TABLE ts_cluster_test(time timestamptz, temp float, location int);
    SELECT table_name from create_hypertable('ts_cluster_test', 'time', chunk_time_interval => interval '1 day');
}

teardown {
    DROP TABLE ts_cluster_test;
}

session "s1"
step "s1a"	{ SELECT pg_sleep(0); }
