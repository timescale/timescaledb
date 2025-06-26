-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Avoid chunkwise aggregation to make the test stable
set timescaledb.enable_chunkwise_aggregation to off;

-- Hypercore TAM uses alternative sparse index predicate pushdown code which
-- lacks the support for the bloom1 sparse index at the moment.
set timescaledb.enable_sparse_index_bloom to off;

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

vacuum analyze readings;

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

-- Testing JSON format to make sure it works and to get coverage for
-- those parts of the code.
explain (analyze, costs off, timing off, summary off, decompress_cache_stats, format json)
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
explain (costs off)
select time, device+device as device_x2 from :chunk limit 1;
select time, device+device as device_x2 from :chunk limit 1;

-- Test sort using Bump memory context on PG17. This didn't use to
-- work on PG17 because it introduced a Bump memory context for
-- per-tuple processing on which compressed data was detoasted. This
-- doesn't work because Bump doesn't support pfree(), which is needed
-- by detoasting.
--
-- Need to convert all chunks to Hypercore TAM.
select compress_chunk(ch, hypercore_use_access_method=>true) from show_chunks('readings') ch;

vacuum analyze readings;

-- Just test that this query doesn't fail with an error about Bump
-- allocator not supporting pfree. Redirect output to a temporary
-- table.
create temp table test_bump as
select * from readings order by time, device;

----------------------------------------------------------------------
--
-- Test scankey push-downs on orderby column. Vectorized filters (or
-- normal qual filters) should remain on all orderby columns, but not
-- segmentby columns.
--
----------------------------------------------------------------------


--
-- Test BETWEEN on time and equality on segmentby
--
explain (costs off)
select * from readings
where time between '2022-06-03' and '2022-06-05' and device = 1;

select sum(humidity) from readings
where time between '2022-06-03' and '2022-06-05' and device = 1;

set timescaledb.enable_hypercore_scankey_pushdown=false;

explain (costs off)
select * from readings
where time between '2022-06-03' and '2022-06-05' and device = 1;

select sum(humidity) from readings
where time between '2022-06-03' and '2022-06-05' and device = 1;

set timescaledb.enable_hypercore_scankey_pushdown=true;

--
-- Test < (LT) on time and > (GT) on segmentby
--
explain (costs off)
select * from readings
where time < '2022-06-05' and device > 5;

select sum(humidity) from readings
where time < '2022-06-05' and device > 5;

set timescaledb.enable_hypercore_scankey_pushdown=false;

explain (costs off)
select * from readings
where time < '2022-06-05' and device > 5;

select sum(humidity) from readings
where time < '2022-06-05' and device > 5;

set timescaledb.enable_hypercore_scankey_pushdown=true;

--
-- Test >= (GE) on time
--
explain (costs off)
select * from readings
where time >= '2022-06-05' and device > 5;

select sum(humidity) from readings
where time >= '2022-06-05' and device > 5;

set timescaledb.enable_hypercore_scankey_pushdown=false;

explain (costs off)
select * from readings
where time >= '2022-06-05' and device > 5;

select sum(humidity) from readings
where time >= '2022-06-05' and device > 5;

set timescaledb.enable_hypercore_scankey_pushdown=true;

--
-- Test = (equality) on time
--
explain (costs off)
select * from readings
where time = '2022-06-01' and 5 < device;

select sum(humidity) from readings
where time = '2022-06-01' and 5 < device;

set timescaledb.enable_hypercore_scankey_pushdown=false;

explain (costs off)
select * from readings
where time = '2022-06-01' and 5 < device;

select sum(humidity) from readings
where time = '2022-06-01' and 4 < device;

set timescaledb.enable_hypercore_scankey_pushdown=true;

--
-- Test scankey push down on non-orderby min/max column
--
explain (costs off)
select * from readings
where time = '2022-06-01' and '5' = location;

select sum(humidity) from readings
where time = '2022-06-01' and '5' = location;

set timescaledb.enable_hypercore_scankey_pushdown=false;

explain (costs off)
select * from readings
where time = '2022-06-01' and '5' = location;

select sum(humidity) from readings
where time = '2022-06-01' and '4' = location;

set timescaledb.enable_hypercore_scankey_pushdown=true;

--
-- Test non-btree operator on segmentby column and compare with btree
-- operators.
--
explain (costs off)
select sum(humidity) from readings
where time <= '2022-06-02' and device <> 1;

select sum(humidity) from readings
where time <= '2022-06-02' and device <> 1;

explain (costs off)
select sum(humidity) from readings
where time <= '2022-06-02' and device != 1;

select sum(humidity) from readings
where time <= '2022-06-02' and device != 1;

set timescaledb.enable_hypercore_scankey_pushdown=false;

explain (costs off)
select sum(humidity) from readings
where time <= '2022-06-02' and device <> 1;

select sum(humidity) from readings
where time <= '2022-06-02' and device <> 1;

set timescaledb.enable_hypercore_scankey_pushdown=true;

--
-- Test non-btree operator on non-segmentby column and compare with
-- btree operators.
--
explain (costs off)
select sum(humidity) from readings
where time <= '2022-06-02' and temp <> 1;

select sum(humidity) from readings
where time <= '2022-06-02' and temp <> 1;

explain (costs off)
select sum(humidity) from readings
where time <= '2022-06-02' and temp != 1;

select sum(humidity) from readings
where time <= '2022-06-02' and temp != 1;

set timescaledb.enable_hypercore_scankey_pushdown=false;

explain (costs off)
select sum(humidity) from readings
where time <= '2022-06-02' and temp <> 1;

select sum(humidity) from readings
where time <= '2022-06-02' and temp <> 1;

set timescaledb.enable_hypercore_scankey_pushdown=true;

--
-- Test "foo IN (1, 2)" (ScalarArrayOpExpr)
--
-- This is currently not transformed to scan keys because only index
-- scans support such keys.
--
explain (costs off)
select * from readings
where time <= '2022-06-02' and device in (1, 4);

select sum(humidity) from readings
where time <= '2022-06-02' and device in (1, 4);

set timescaledb.enable_hypercore_scankey_pushdown=false;

explain (costs off)
select * from readings
where time <= '2022-06-02' and device in (1, 4);

select sum(humidity) from readings
where time <= '2022-06-02' and device in (1, 4);

set timescaledb.enable_hypercore_scankey_pushdown=true;

-- ScalarArrayOpExpr "foo IN (1, 2)" are pushed down to compressed
-- chunk with transparent decompression. As noted above, with TAM,
-- such expressions are not pushed down as scan keys because only
-- index scans support such keys.
set timescaledb.enable_transparent_decompression='hypercore';

explain (costs off)
select * from readings
where time <= '2022-06-02' and device in (1, 4);

select sum(humidity) from readings
where time <= '2022-06-02' and device in (1, 4);

set timescaledb.enable_transparent_decompression=true;

--
-- Test filter that doesn't reference a column.
--
select setseed(0.1);
explain (costs off)
select * from readings
where time <= '2022-06-02' and ceil(random()*6)::int = 2;

select sum(humidity) from readings
where time <= '2022-06-02' and ceil(random()*6)::int = 2;


--
-- Test filter that doesn't have a Const on left or right side.
--
explain (costs off)
select * from readings
where time <= '2022-06-02' and location::int = device;

select sum(humidity) from readings
where time <= '2022-06-02' and location::int = device;

--
-- Test filter that does coercing (CoerceViaIO) type. Not pushed down
-- as scankey.
--
explain (costs off)
select * from readings
where time <= '2022-06-02' and '1' = device::text;

select sum(humidity) from readings
where time <= '2022-06-02' and '1' = device::text;

--
-- Test filter that does relabeling (RelabelType) for binary
-- compatible types, both left and right side of expression.
--
explain (costs off)
select * from readings
where time <= '2022-06-02' and '1'::oid = device;

select sum(humidity) from readings
where time <= '2022-06-02' and '1'::oid = device;

explain (costs off)
select sum(humidity) from readings
where time <= '2022-06-02' and device = '1'::oid;

select sum(humidity) from readings
where time <= '2022-06-02' and device = '1'::oid;

--
-- Test backwards scan with segmentby and vector quals
--
select count(*)-4 as myoffset from readings
where time <= '2022-06-02' and device in (1, 2)
\gset

-- Get the last four values to compare with cursor fetch backward from
-- the end
select * from readings
where time <= '2022-06-02' and device in (1, 2)
offset :myoffset;

begin;
declare cur1 scroll cursor for
select * from readings
where time <= '2022-06-02' and device in (1, 2);
move last cur1;
-- move one step beyond last
fetch forward 1 from cur1;
-- fetch the last 4 values with two fetches
fetch backward 2 from cur1;
fetch backward 2 from cur1;
close cur1;
commit;

reset timescaledb.enable_chunkwise_aggregation;
reset timescaledb.enable_sparse_index_bloom;
