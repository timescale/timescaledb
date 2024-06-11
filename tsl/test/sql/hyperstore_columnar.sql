-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/hyperstore_helpers.sql

create table readings(
       metric_id serial,
       time timestamptz unique,
       location int,
       device int,
       temp numeric(4,1),
       humidity float
);

select create_hypertable('readings', 'time');
-- Disable incremental sort to make tests stable
set enable_incremental_sort = false;
select setseed(1);

insert into readings (time, location, device, temp, humidity)
select t, ceil(random()*10), ceil(random()*30), random()*40, random()*100
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5m') t;

alter table readings SET (
	  timescaledb.compress,
	  timescaledb.compress_orderby = 'time',
	  timescaledb.compress_segmentby = 'device'
);

-- Set some test chunks as global variables
select format('%I.%I', chunk_schema, chunk_name)::regclass as chunk
  from timescaledb_information.chunks
 where format('%I.%I', hypertable_schema, hypertable_name)::regclass = 'readings'::regclass
 limit 1 \gset

alter table :chunk set access method hyperstore;

-- Test that filtering is not removed on ColumnarScan when it includes
-- columns that cannot be scankeys.
select explain_analyze_anonymize(format($$
       select * from %s where device < 4 and location = 2 limit 5
$$, :'chunk'));

-- Save away all data from the chunk so that we can compare.
create table saved as select * from :chunk;

-- Check that we have matching rows in both.
with
  lhs as (select * from :chunk),
  rhs as (select * from saved)
select lhs.*, rhs.*
from lhs full join rhs using (metric_id)
where lhs.metric_id is null or rhs.metric_id is null;

--
-- Test vectorized filters on compressed column.
--

-- Use a filter that filters all rows in order to test that the scan
-- does not decompress more than necessary to filter data. The
-- decompress count should be equal to the number cache hits (i.e., we
-- only decompress one column per segment).
select explain_analyze_anonymize(format($$
       select count(*) from %s where humidity > 110
$$, :'chunk'));
select count(*) from :chunk where humidity > 110;

-- Test with a query that should generate some rows. Make sure it
-- matches the result of a normal table.
select explain_analyze_anonymize(format($$
       select count(*) from %s where humidity > 50
$$, :'chunk'));

select lhs.count, rhs.count
from (select count(*) from :chunk where humidity > 50) lhs,
     (select count(*) from saved where humidity > 50) rhs;

with
  lhs as (select * from :chunk where humidity > 50),
  rhs as (select * from saved where humidity > 50)
select lhs.*, rhs.*
from lhs full join rhs using (metric_id)
where lhs.metric_id is null or rhs.metric_id is null;

-- Test that a type that a type that does not support batch
-- decompression (numeric in this case) behaves as expected.
select explain_analyze_anonymize(format($$
       select count(*) from %s where temp > 50
$$, :'chunk'));
select count(*) from :chunk where temp > 50;

-- test same thing with a query that should generate some rows.
select explain_analyze_anonymize(format($$
       select count(*) from %s where temp > 20
$$, :'chunk'));

select lhs.count, rhs.count
from (select count(*) from :chunk where temp > 20) lhs,
     (select count(*) from saved where temp > 20) rhs;

with
  lhs as (select * from :chunk where temp > 20),
  rhs as (select * from saved where temp > 20)
select lhs.*, rhs.*
from lhs full join rhs using (metric_id)
where lhs.metric_id is null or rhs.metric_id is null;

-- test with clauses that use both vectorizable and non-vectorizable
-- types together.
explain (analyze, costs off, timing off, summary off, decompress_cache_stats)
select count(*) from :chunk where humidity > 40 and temp > 20;
select count(*) from :chunk where humidity > 40 and temp > 20;

select lhs.count, rhs.count
from (select count(*) from :chunk where humidity > 40 and temp > 20) lhs,
     (select count(*) from saved where humidity > 40 and temp > 20) rhs;

-- test scans with clasues that are vectorizable, non-vectorizable,
-- and used as scan key.
select explain_analyze_anonymize(format($$
       select count(*) from %s where humidity > 40 and temp > 20 and device = 3
$$, :'chunk'));
select count(*) from :chunk where humidity > 40 and temp > 20 and device = 3;

select lhs.count, rhs.count from
  (select count(*) from :chunk where humidity > 40 and temp > 20 and device = 3) as lhs,
  (select count(*) from saved where humidity > 40 and temp > 20 and device = 3) as rhs;

-- test that columnar scan can be turned off
set timescaledb.enable_columnarscan = false;
select explain_analyze_anonymize(format($$
       select * from %s where device < 4 order by device asc limit 5
$$, :'chunk'));

drop table readings;
drop table saved;

