-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table readings(time timestamptz,
       location text,
       device int,
       temp float,
       humidity float,
       unique (device, location, time)
);

create index on readings(location);
create index on readings(device);

select create_hypertable('readings', 'time');

select setseed(1);

insert into readings (time, location, device, temp, humidity)
select t, ceil(random()*3), ceil(random()*30), random()*40, random()*100
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5m') t;

alter table readings set (
      timescaledb.compress,
      timescaledb.compress_orderby = 'time',
      timescaledb.compress_segmentby = 'device'
);

select format('%I.%I', chunk_schema, chunk_name)::regclass as chunk
  from timescaledb_information.chunks
 where format('%I.%I', hypertable_schema, hypertable_name)::regclass = 'readings'::regclass
 limit 1 \gset

alter table :chunk set access method hypercore;

--
-- Check that TID scan works for both compressed and non-compressed
-- rows.
--

set timescaledb.enable_transparent_decompression to false;

-- Select any row and try to fetch it using CTID. We do not select the
-- first one just to also try to scan a few rows and make sure the
-- implementation works.
select ctid from :chunk limit 1 offset 10 \gset

explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select * from :chunk where ctid = :'ctid';
select * from :chunk where ctid = :'ctid';

-- Insert a new row, which will then be non-compressed, and fetch it.
insert into :chunk values ('Wed May 25 17:34:56 2022 PDT', 1, 2, 3.14, 2.14);
select ctid from :chunk where time = 'Wed May 25 17:34:56 2022 PDT' \gset

explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select * from :chunk where ctid = :'ctid';
select * from :chunk where ctid = :'ctid';

-- Check that a bad option name generates an error.
\set ON_ERROR_STOP 0
explain (analyze, costs off, timing off, summary off, decopress_cache_stats)
select * from :chunk where device between 5 and 10;
\set ON_ERROR_STOP 1

explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select time, temp + humidity from readings where device between 5 and 10 and humidity > 5;

-- Check the explain cache information output.
--
-- Query 1 and 3 should show the same explain plan, and the plan in
-- the middle should not include decompress stats:
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select time, temp + humidity from readings where device between 5 and 10 and humidity > 5;

-- Check the explain cache information output. Query 1 and 3 should
-- show the same explain plan, and the plan in the middle should not
-- include decompress stats:
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select * from :chunk where device between 5 and 10;
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select * from :chunk where device between 5 and 10;

-- Queries that will select just a few columns
set max_parallel_workers_per_gather to 0;
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select device, humidity from readings where device between 5 and 10;

explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select device, avg(humidity) from readings where device between 5 and 10
group by device;

-- Test on conflict: insert the same data as before, but throw away
-- the updates.
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
insert into readings (time, location, device, temp, humidity)
select t, ceil(random()*10), ceil(random()*30), random()*40, random()*100
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5m') t
on conflict (location, device, time) do nothing;

-- This should show values for all columns
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select time, temp + humidity from readings where device between 5 and 10 and humidity > 5 limit 5;
select time, temp + humidity from readings where device between 5 and 10 and humidity > 5 limit 5;

-- Get the compressed chunk
select format('%I.%I', c2.schema_name, c2.table_name)::regclass as cchunk
from _timescaledb_catalog.chunk c1
join _timescaledb_catalog.chunk c2
on (c1.compressed_chunk_id = c2.id)
where format('%I.%I', c1.schema_name, c1.table_name)::regclass = :'chunk'::regclass \gset

-- Show that location is using dictionary encoding
select (_timescaledb_functions.compressed_data_info(location)).* from :cchunk limit 1;

-- Test that vectorized filtering on text column works
set enable_indexscan=off;
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select time, location, temp from :chunk
where location = 1::text
order by time desc;

--  Save the data for comparison with seqscan
create temp table chunk_saved as
select time, location, temp from :chunk
where location = 1::text
order by time desc;

-- Show same query with seqscan and compare output
set timescaledb.enable_columnarscan=off;
explain (analyze, costs off, timing off, summary off)
select time, location, temp from :chunk
where location = 1::text
order by time desc;

-- If output is the same, this query should return nothing
(select time, location, temp from :chunk
where location = 1::text
order by time desc)
except
select * from chunk_saved;

-- Insert some non-compressed values to see that vectorized filtering
-- works on those non-compressed text columns.
insert into :chunk values ('2022-06-01 15:30'::timestamptz, 1, 2, 3.14, 2.14), ('2022-06-01 15:30'::timestamptz, 2, 2, 3.14, 2.14);

-- Query should only return the one non-compressed row that has location=1
(select time, location, temp from :chunk
where location = 1::text
order by time desc)
except
select * from chunk_saved;

-- Test that a ColumnarScan doesn't decompress anything if there are
-- no referenced columns, or the referenced column is a segmentby
-- column
set timescaledb.enable_columnarscan=true;
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select count(*) from :chunk where device = 1;

explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select device from :chunk where device = 1;

-- Using a non-segmentby column will decompress that column
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select count(*) from :chunk where location = 1::text;

-- Testing same thing with SeqScan. It still decompresses in the
-- count(*) case, although it shouldn't have to. So, probably an
-- opportunity to optimize.
set timescaledb.enable_columnarscan=false;
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select count(*) from :chunk where device = 1;

explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select device from :chunk where device = 1;

explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select count(*) from :chunk where location = 1::text;

-- ColumnarScan declares itself as projection capable. This query
-- would add a Result node on top if ColumnarScan couldn't project.
set timescaledb.enable_columnarscan=true;
explain
select time, device+device as device_x2 from :chunk limit 1;
select time, device+device as device_x2 from :chunk limit 1;

