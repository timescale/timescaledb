# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

setup {
	create table metrics (
		metric_id serial,
		created_at timestamptz not null,
		device_id int,
		temp float
	);
	select from create_hypertable('metrics', by_range('created_at'), create_default_indexes=>false);

	select setseed(1);

	insert into metrics (created_at, device_id, temp)
	select t, ceil(random()*30), random()*100
	from generate_series('2022-06-01'::timestamptz, '2022-06-10', '2m') t;

	alter table metrics set (timescaledb.compress, timescaledb.compress_segmentby='device_id');

	-- Convert to hyperstore and give chunks predictible names
	do $$
	declare
	   chunk regclass;
	   count int = 1;
	begin
	   for chunk in select ch from show_chunks('metrics') ch loop
		   execute format('alter table %s set access method hyperstore', chunk);
		   execute format('alter table %s rename to test_chunk_%s', chunk, count);
		   count = count + 1;
	   end loop;
	end;
	$$;

	create index metrics_temp_idx on metrics (temp);
	create index metrics_created_at_idx on metrics (created_at);
	create index metrics_device_id_idx on metrics (device_id);

	create extension pgstattuple;
}

teardown {
	drop table metrics;
}

session "s1"
step "s1_start_transaction" {
	start transaction;
}

step "s1_commit" {
	commit;
}
step "s1_select" {
	select count(*) from _timescaledb_internal.test_chunk_1 where metric_id = 10;
}

session "s2"
step "s2_vacuum" {
	vacuum _timescaledb_internal.test_chunk_1;
}

session "s3"
step "s3_delete_half" {
	delete from metrics where device_id > 15;
}
step "s3_select" {
	select count(*) from metrics where temp > 20;
	select tuple_count as tuples_temp_idx, dead_tuple_count as dead_tuples_temp_idx
	  from pgstattuple('_timescaledb_internal.test_chunk_1_metrics_temp_idx');
	select tuple_count as tuples_device_id_idx, dead_tuple_count as dead_tuples_device_id_idx
	  from pgstattuple('_timescaledb_internal.test_chunk_1_metrics_device_id_idx');
}

# Test concurrent vacuum and delete, with a reading third
# transaction. We'd expect first vacuum to fail to clean up tuples
# because there is an ongoing read transaction. However, when that
# read commits, vacuum should succeed in cleaning up.
permutation "s3_select" "s1_start_transaction" "s1_select" "s3_delete_half" "s3_select" "s2_vacuum" "s3_select" "s1_commit" "s3_select" "s2_vacuum" "s3_select"
