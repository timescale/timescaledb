-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--
-- Test VectorAgg on top of scans with Hypercore TAM.
--
-- This test is not for the vectorized aggregation functionality
-- itself, since that is handled by other tests. Here we only test
-- that VectorAgg is compatible with scans on Hypercore TAM and that
-- the planning is done correctly.
--
-- To run on a larger data set, the vectorized_aggregation and
-- vector_agg_* tests can be run with TAM enabled by default. This is
-- also a good way to verify that the output from Hypercore TAM is the
-- same.
--
create table aggdata (time timestamptz, device int, location int, temp float);
select create_hypertable('aggdata', 'time', create_default_indexes=>false);
insert into aggdata values ('2024-01-01 01:00', 1, 1, 1.0), ('2024-01-01 01:00', 2, 1, 2.0), ('2024-03-01 01:00', 3, 2, 3.0), ('2024-01-01 02:00', NULL, 1, 0.0), ('2024-01-01 02:00', NULL, 3, NULL);

select format('%I.%I', chunk_schema, chunk_name)::regclass as chunk
from timescaledb_information.chunks
where hypertable_name='aggdata'
limit 1 \gset

alter table aggdata set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='device');
alter table :chunk set access method hypercore;

-- Add some non-compressed data to ensure vectoraggs work with both
-- compressed and non-compressed data.
insert into aggdata values ('2024-01-01 02:00', 1, 1, 3.0);

analyze aggdata;
--
-- Run a number of queries to compare plans and output with vectorized
-- aggregation on and off.
--
-- This is just to do basic checks to ensure VectorAggs produce the
-- expected output.
--
set timescaledb.debug_require_vector_agg to 'require';
explain (verbose, costs off)
select avg(device) from aggdata;
select avg(device) from aggdata;

set timescaledb.enable_vectorized_aggregation=false;
reset timescaledb.debug_require_vector_agg;
explain (verbose, costs off)
select avg(device) from aggdata;
select avg(device) from aggdata;

--
-- Test agg filter on segmentby column
--
set timescaledb.enable_vectorized_aggregation=true;
explain (verbose, costs off)
select avg(temp) filter (where device > 1) from aggdata;
set timescaledb.debug_require_vector_agg to 'require';
select avg(temp) filter (where device > 1) from aggdata;

set timescaledb.enable_vectorized_aggregation=false;
reset timescaledb.debug_require_vector_agg;
explain (verbose, costs off)
select avg(temp) filter (where device > 1) from aggdata;
select avg(temp) filter (where device > 1) from aggdata;

--
-- Test agg filter on non-segmentby column
--
set timescaledb.enable_vectorized_aggregation=true;
explain (verbose, costs off)
select count(*) filter (where location < 3) from aggdata;
set timescaledb.debug_require_vector_agg to 'require';
select count(*) filter (where location < 3) from aggdata;

set timescaledb.enable_vectorized_aggregation=false;
reset timescaledb.debug_require_vector_agg;
explain (verbose, costs off)
select count(*) filter (where location < 3) from aggdata;
select count(*) filter (where location < 3) from aggdata;

--
-- Test grouping on non-segmentby column
--
set timescaledb.enable_vectorized_aggregation=true;
explain (verbose, costs off)
select location, avg(temp) from aggdata where location=1 group by location;
set timescaledb.debug_require_vector_agg to 'require';
select location, avg(temp) from aggdata where location=1 group by location;

set timescaledb.enable_vectorized_aggregation=false;
reset timescaledb.debug_require_vector_agg;
explain (verbose, costs off)
select location, avg(temp) from aggdata where location=1 group by location;
select location, avg(temp) from aggdata where location=1 group by location;

set timescaledb.enable_vectorized_aggregation=true;
explain (verbose, costs off)
select location, count(*) from aggdata where location=1 group by location;
set timescaledb.debug_require_vector_agg to 'require';
select location, count(*) from aggdata where location=1 group by location;

set timescaledb.enable_vectorized_aggregation=false;
reset timescaledb.debug_require_vector_agg;
explain (verbose, costs off)
select location, count(*) from aggdata where location=1 group by location;
select location, count(*) from aggdata where location=1 group by location;

--
-- Test ordering/grouping on segmentby, orderby columns
--
set timescaledb.enable_vectorized_aggregation=true;
explain (verbose, costs off)
select time, device, sum(temp) from aggdata where device is not null group by time, device order by time, device limit 10;
select time, device, sum(temp) from aggdata where device is not null group by time, device order by time, device limit 10;

set timecaledb.enable_vectorized_aggregation=false;
explain (verbose, costs off)
select time, device, sum(temp) from aggdata where device is not null group by time, device order by time, device limit 10;
select time, device, sum(temp) from aggdata where device is not null group by time, device order by time, device limit 10;

set timescaledb.enable_vectorized_aggregation=true;
explain (verbose, costs off)
select time, device, sum(temp) filter (where device is not null) from aggdata group by time, device order by time, device desc limit 10;
select time, device, sum(temp) filter (where device is not null) from aggdata group by time, device order by time, device desc limit 10;

set timescaledb.enable_vectorized_aggregation=false;
explain (verbose, costs off)
select time, device, sum(temp) filter (where device is not null) from aggdata group by time, device order by time, device desc limit 10;
select time, device, sum(temp) filter (where device is not null) from aggdata group by time, device order by time, device desc limit 10;

--
-- Test ordering on time (orderby), ASC as well as DESC
--
set timescaledb.enable_vectorized_aggregation=true;
explain (verbose, costs off)
select time, sum(temp) from aggdata group by time order by time limit 10;
set timescaledb.debug_require_vector_agg to 'require';
select time, sum(temp) from aggdata group by time order by time limit 10;

set timescaledb.enable_vectorized_aggregation=false;
reset timescaledb.debug_require_vector_agg;
explain (verbose, costs off)
select time, sum(temp) from aggdata group by time order by time limit 10;
select time, sum(temp) from aggdata group by time order by time limit 10;


set timescaledb.enable_vectorized_aggregation=true;
explain (verbose, costs off)
select time, sum(temp) from aggdata group by time order by time desc limit 10;
set timescaledb.debug_require_vector_agg to 'require';
select time, sum(temp) from aggdata group by time order by time desc limit 10;

set timescaledb.enable_vectorized_aggregation=false;
reset timescaledb.debug_require_vector_agg;
explain (verbose, costs off)
select time, sum(temp) from aggdata group by time order by time desc limit 10;
select time, sum(temp) from aggdata group by time order by time desc limit 10;

--
-- Test ordering on time (orderby), ASC as well as DESC with no segmentby
--
create table aggdata_timeorder (like aggdata);
select create_hypertable('aggdata_timeorder', 'time', create_default_indexes=>false);
insert into aggdata_timeorder select * from aggdata;

select format('%I.%I', chunk_schema, chunk_name)::regclass as chunk
from timescaledb_information.chunks
where hypertable_name='aggdata_timeorder'
limit 1 \gset

alter table aggdata_timeorder set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='');
alter table :chunk set access method hypercore;
analyze aggdata_timeorder;

set timescaledb.enable_vectorized_aggregation=true;
explain (verbose, costs off)
select time, sum(temp) from aggdata_timeorder group by time order by time limit 10;
set timescaledb.debug_require_vector_agg to 'require';
select time, sum(temp) from aggdata_timeorder group by time order by time limit 10;

explain (verbose, costs off)
select time, sum(temp) from aggdata_timeorder group by time order by time desc limit 10;
select time, sum(temp) from aggdata_timeorder group by time order by time desc limit 10;
