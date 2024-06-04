-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table readings (time timestamptz not null, device int, temp float);
select create_hypertable('readings', 'time', create_default_indexes => false, chunk_time_interval => '1 month'::interval);
alter table readings set (timescaledb.compress_segmentby = 'device');

insert into readings (time, device, temp)
select t, ceil(random()*10), ceil(random()*30)
from generate_series('2022-06-01'::timestamptz, '2022-06-10', '10m') t;

create index on readings (time);
create index on readings (device);

select count(*) from show_chunks('readings');

create view chunk_info as
select hypertable_name as hypertable, chunk_name as chunk, amname, is_compressed
from timescaledb_information.chunks ch
join pg_class cl on (format('%I.%I', ch.chunk_schema, ch.chunk_name)::regclass = cl.oid)
join pg_am am on (am.oid = cl.relam);

\set ON_ERROR_STOP 0
-- Test invalid compress_using option
select add_compression_policy('readings',
                              compress_after => '1000 years'::interval,
                              compress_using => 'foo');
\set ON_ERROR_STOP 1

-- Check that compress_using is not part of the policy if not set. Use
-- a large compress_after to ensure the policy doesn't do anything at
-- this time.
select add_compression_policy('readings', compress_after => '1000 years'::interval)
as compression_job \gset
select config from timescaledb_information.jobs where job_id = :compression_job;
select remove_compression_policy('readings');

-- Check that compress_using is not part of the policy if set to NULL
select add_compression_policy('readings',
                              compress_after => '1000 years'::interval,
                              compress_using => NULL)
as compression_job \gset
select config from timescaledb_information.jobs where job_id = :compression_job;
select remove_compression_policy('readings');

-- All chunks are heap before policy run
select * from chunk_info
where hypertable = 'readings'
order by chunk;

-- Check that compress_using is part of the policy config when non-NULL
select add_compression_policy('readings',
                              compress_after => '1 day'::interval,
                              compress_using => 'hyperstore')
as compression_job \gset

select config from timescaledb_information.jobs where job_id = :compression_job;

-- Make sure the policy runs
call run_job(:'compression_job');

-- After policy run all the chunks should be hyperstores
select * from chunk_info
where hypertable = 'readings'
order by chunk;

select remove_compression_policy('readings');

-- Insert a new value, which will not be compressed
insert into readings values ('2022-06-01 10:14', 1, 1.0);

-- Show that the value is not compressed
select _timescaledb_debug.is_compressed_tid(ctid), *
from readings
where time = '2022-06-01 10:14' and device = 1;

-- Add a new policy that doesn't specify hyperstore. It should still
-- recompress hyperstores.
select add_compression_policy('readings',
                              compress_after => '1 day'::interval,
                              compress_using => 'heap')
as compression_job \gset

-- Run the policy job again to recompress
call run_job(:'compression_job');

-- The value should now be compressed
select _timescaledb_debug.is_compressed_tid(ctid), *
from readings
where time = '2022-06-01 10:14' and device = 1;

-- Query via index scan
explain (costs off)
select * from readings where time = '2022-06-01 10:14' and device = 1;
select * from readings where time = '2022-06-01 10:14' and device = 1;

-- Test recompression again with a policy that doesn't specify
-- compress_using
select remove_compression_policy('readings');
-- Insert one value into existing hyperstore, also create a new non-hyperstore chunk
insert into readings values ('2022-06-01 10:14', 1, 1.0), ('2022-07-01 10:14', 2, 2.0);

-- The new chunk should be heap and not compressed
select * from chunk_info
where hypertable = 'readings'
order by chunk;

select add_compression_policy('readings',
                              compress_after => '1 day'::interval)
as compression_job \gset

-- Run the policy job to recompress hyperstores and compress the new
-- chunk using non-hyperstore compression
call run_job(:'compression_job');

select * from chunk_info
where hypertable = 'readings'
order by chunk;

select remove_compression_policy('readings');

-- Test 1-step policies on caggs
create materialized view daily (day, device, avg_temp)
with (timescaledb.continuous) as
select time_bucket('1 day'::interval, time) as day, device, avg(temp)
from readings
group by day, device;

alter materialized view daily set (timescaledb.compress_segmentby = 'device');
select timescaledb_experimental.add_policies('daily',
       refresh_start_offset => '8 days'::interval,
       refresh_end_offset => '1 day'::interval,
       compress_after => '9 days'::interval,
       compress_using => 'hyperstore');

select job_id as cagg_compression_job, materialization_hypertable_name as mathyper
from timescaledb_information.jobs j
join timescaledb_information.continuous_aggregates ca
on (ca.materialization_hypertable_name = j.hypertable_name)
where view_name = 'daily' and proc_name = 'policy_compression' \gset


select * from chunk_info
where hypertable = :'mathyper'
order by chunk;

call run_job(:'cagg_compression_job');

select * from chunk_info
where hypertable = :'mathyper'
order by chunk;
