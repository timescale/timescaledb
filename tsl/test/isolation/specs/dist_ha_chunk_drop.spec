# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

#
# Test concurrent insert into dist hypertable after a data node marked
# as unavailable would produce 'tuple concurrently deleted` error.
#
# The problem occurs because of missing tuple level locking during scan and concurrent
# delete from chunk_data_node table afterwards, which should be treated as
# `SELECT … FOR UPDATE`.
#
setup
{
	CREATE TABLE metric1(ts TIMESTAMPTZ NOT NULL, val FLOAT8 NOT NULL, dev_id INT4 NOT NULL);
}

setup { SELECT node_name FROM add_data_node('data_node_1', host => 'localhost', database => 'cdha_1', if_not_exists => true); }
setup { SELECT node_name FROM add_data_node('data_node_2', host => 'localhost', database => 'cdha_2', if_not_exists => true); }
setup { SELECT node_name FROM add_data_node('data_node_3', host => 'localhost', database => 'cdha_3', if_not_exists => true); }
setup { SELECT node_name FROM add_data_node('data_node_4', host => 'localhost', database => 'cdha_4', if_not_exists => true); }
setup { SELECT created FROM create_distributed_hypertable('metric1', 'ts', 'dev_id', chunk_time_interval => INTERVAL '1 hour', replication_factor => 4); }

teardown
{
   DROP TABLE metric1;
}

# bootstrap cluster with data
session "s1"
setup
{
	SET application_name = 's1';
}
step "s1_init"            { INSERT INTO metric1(ts, val, dev_id) SELECT s.*, 3.14, d.* FROM generate_series('2021-08-17 00:00:00'::timestamp, '2021-08-17 00:00:59'::timestamp, '1 s'::interval) s CROSS JOIN generate_series(1, 500) d; }
step "s1_set_unavailable" { SELECT alter_data_node('data_node_4', available=>false); }
step "s1_set_available"   { SELECT alter_data_node('data_node_4', available=>true); }
step "s1_insert"          { INSERT INTO metric1(ts, val, dev_id) SELECT s.*, 3.14, d.* FROM generate_series('2021-08-17 00:01:00'::timestamp, '2021-08-17 00:01:59'::timestamp, '1 s'::interval) s CROSS JOIN generate_series(1, 249) d; }

# concurrent session
session "s2"
setup
{
	SET application_name = 's2';
}
step "s2_insert"          { INSERT INTO metric1(ts, val, dev_id) SELECT s.*, 3.14, d.* FROM generate_series('2021-08-17 00:01:00'::timestamp, '2021-08-17 00:01:59'::timestamp, '1 s'::interval) s CROSS JOIN generate_series(250, 499) d; }

# locking session
session "s3"
setup
{
	SET application_name = 's3';
}
step "s3_lock_enable"   { SELECT debug_waitpoint_enable('chunk_data_node_delete'); }
step "s3_lock_release"  { SELECT debug_waitpoint_release('chunk_data_node_delete'); }

permutation "s1_init" "s1_set_unavailable" "s3_lock_enable" "s1_insert" "s2_insert" "s3_lock_release" "s1_set_available"
