-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/hypercore_helpers.sql
select setseed(0.3);

-- Testing the basic API for creating a hypercore

-- This should just fail because you cannot create a plain table with
-- hypercore (yet).
\set ON_ERROR_STOP 0
\set VERBOSITY default
create table test2(
       created_at timestamp with time zone not null,
       location_id int
) using hypercore;

set default_table_access_method to 'hypercore';
create table test2(
       created_at timestamp with time zone not null,
       location_id int
);

reset default_table_access_method;
\set VERBOSITY terse
\set ON_ERROR_STOP 1

CREATE TABLE test2(
	   created_at timestamptz not null,
	   location_id int,
	   device_id int,
	   temp float,
	   humidity float
);

create index on test2(device_id, created_at);

\set ON_ERROR_STOP 0
alter table test2 set access method hypercore;
\set ON_ERROR_STOP 1

select create_hypertable('test2', 'created_at');

\set ON_ERROR_STOP 0
-- Should show error since there is no namespace.
alter table test2
	  set access method hypercore,
	  set (compress_segmentby = 'location_id');
\set ON_ERROR_STOP 1

alter table test2
	  set access method hypercore,
	  set (timescaledb.compress_segmentby = 'location_id');

-- Test altering hypertable to hypercore again. It should be allowed
-- and be a no-op.
alter table test2 set access method hypercore;

\set ON_ERROR_STOP 0
-- This shows an error but the error is weird, we should probably get
-- a better one.
alter table test2
	  set access method hypercore,
	  set (compress_segmentby = 'location_id');
\set ON_ERROR_STOP 1

-- Create view for hypercore rels
create view amrels as
select cl.oid::regclass as rel, am.amname, inh.inhparent::regclass as relparent
  from pg_class cl
  inner join pg_am am on (cl.relam = am.oid)
  left join pg_inherits inh on (inh.inhrelid = cl.oid);

-- Show that test2 is a hypercore
select rel, amname
from amrels
where rel='test2'::regclass;

-- This will create new chunks for the hypertable
insert into test2 (created_at, location_id, device_id, temp, humidity)
select t, ceil(random()*10), ceil(random()*30), random()*40, random()*100
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5m') t;

-- Save the count for test2 for later comparison
select count(*) as orig_test2_count from test2 \gset

-- All chunks should use the hypercore access method
select * from amrels
where relparent='test2'::regclass;

-- Show compression settings for hypercore across catalog and views
select * from _timescaledb_catalog.compression_settings;
select * from timescaledb_information.compression_settings;
select * from timescaledb_information.chunk_compression_settings;

--------------------------
-- Test alter on chunks --
--------------------------
create table test3 (time timestamptz not null, device int, temp float);
select create_hypertable('test3', 'time');

-- create one chunk
insert into test3 values ('2022-06-01', 1, 1.0);

-- save chunk as variable
select ch as chunk from show_chunks('test3') ch limit 1 \gset

-- Check that chunk is NOT using hypercore
select rel, amname
from amrels
where relparent='test3'::regclass;

\set ON_ERROR_STOP 0
-- Cannot create hypercore if missing compression settings
alter table :chunk set access method hypercore;
\set ON_ERROR_STOP 1

-- Add compression settings
alter table test3 set (timescaledb.compress, timescaledb.compress_orderby='time desc', timescaledb.compress_segmentby='');
alter table :chunk set access method hypercore;

-- Check that chunk is using hypercore
select * from amrels where rel=:'chunk'::regclass;

-- Try same thing with compress_chunk()
alter table :chunk set access method heap;
select compress_chunk(:'chunk', compress_using => 'hypercore');

-- Check that chunk is using hypercore
select relname, amname
  from show_chunks('test3') as chunk
  join pg_class on (pg_class.oid = chunk)
  join pg_am on (relam = pg_am.oid);

-- Test setting same access method again
alter table :chunk set access method hypercore;

-- Test recompression after changing compression settings
alter table test3 set (timescaledb.compress_segmentby='device');
select compress_chunk(:'chunk', compress_using => 'hypercore', recompress => true);

-- Create a second chunk
insert into test3 values ('2022-08-01', 1, 1.0);

-- The second chunk should not be a hypercore chunk
select * from amrels where relparent='test3'::regclass;

-- Set hypercore on hypertable
alter table test3 set access method hypercore;

-- Create a third chunk
insert into test3 values ('2022-10-01', 1, 1.0);

-- The third chunk should be a hypercore chunk
select * from amrels where relparent='test3'::regclass;

-- Test that we can DDL on a hypertable that is not a Hypercore but
-- has one chunk that is a Hypercore works.
create table test4 (time timestamptz not null, device int, temp float);
select created from create_hypertable('test4', 'time');

insert into test4 values ('2022-06-01', 1, 1.0), ('2022-08-01', 1, 1.0);
-- should be at least two chunks
select count(ch) from show_chunks('test4') ch;
select ch as chunk from show_chunks('test4') ch limit 1 \gset

alter table test4 set (timescaledb.compress);
alter table :chunk set access method hypercore;
select * from amrels where relparent='test4'::regclass;

-- test that alter table on the hypertable works
alter table test4 add column magic int;

\d :chunk

-- Test that dropping a table with one chunk being a hypercore works.
drop table test4;

-- Create view to see compression stats. Left join chunks with stats
-- to detect missing stats. Only show row counts because size stats
-- seem to vary in tests
create view compressed_rel_size_stats as
select
	cl.oid::regclass as rel,
	am.amname,
	inh.inhparent::regclass as relparent,
	numrows_pre_compression,
	numrows_post_compression,
	numrows_frozen_immediately
from  _timescaledb_catalog.chunk c
left join _timescaledb_catalog.compression_chunk_size ccs
	  on (c.id = ccs.chunk_id)
inner join pg_class cl
	  on (cl.oid = format('%I.%I', c.schema_name, c.table_name)::regclass)
inner join pg_am am
	  on (am.oid = cl.relam)
inner join pg_inherits inh
	  on (inh.inhrelid = cl.oid)
where c.compressed_chunk_id is not null;

-- There should be no hypercore chunks that lack compression size stats
select count(*) as num_stats_missing from compressed_rel_size_stats
where amname = 'hypercore' and numrows_pre_compression is null;

-- Show stats for hypercore chunks. Note that many stats are 0 since
-- chunks were created as a result of inserts and not really
-- compressed
select * from compressed_rel_size_stats order by rel;

-- Decompress hypercores to check that stats are removed
select decompress_chunk(rel)
  from compressed_rel_size_stats
  where amname = 'hypercore';

-- All stats should be removed
select count(*) as orphaned_stats
from compressed_rel_size_stats;

-- Create hypercores again and check that compression size stats are
-- updated showing compressed data
select compress_chunk(ch, compress_using => 'hypercore')
from show_chunks('test2') ch;
select compress_chunk(ch, compress_using => 'hypercore')
from show_chunks('test3') ch;

-- Save the stats for later comparison. Exclude the amname column
-- since it will differ.
create table saved_stats as
select
	rel,
	relparent,
	numrows_pre_compression,
	numrows_post_compression,
	numrows_frozen_immediately
from compressed_rel_size_stats;

select * from compressed_rel_size_stats order by rel;

-- Convert back to heap and compress the old way to compare
-- compression size stats
select compress_chunk(decompress_chunk(ch))
from show_chunks('test2') ch;
-- Using compress_using => NULL should be the same as "heap"
select compress_chunk(decompress_chunk(ch), compress_using => NULL)
from show_chunks('test3') ch;

select * from compressed_rel_size_stats order by rel;

-- Check that stats are the same for hypercore and now with
-- compression. Should return zero rows if they are the same.
select
	rel,
	relparent,
	numrows_pre_compression,
	numrows_post_compression,
	numrows_frozen_immediately
from compressed_rel_size_stats
except
select * from saved_stats;

-- Try migration to hypercore directly from compressed heap. Run in a
-- transaction block to make sure changes are visible to following
-- commands.
begin;

-- Check pg_am dependencies for the chunks. Since they are using heap
-- AM, there should be no dependencies as heap AM is always present.
select dep.objid::regclass, am.amname
from show_chunks('test2') ch
join pg_depend dep on (ch = dep.objid)
join pg_am am on (dep.refobjid = am.oid);

-- Use DEBUG2 to show that migration path is invoked
set client_min_messages=DEBUG1;
with chunks as (
	 select ch from show_chunks('test2') ch offset 1
)
select compress_chunk(ch, compress_using => 'hypercore') from chunks;

-- Test direct migration of the remaining chunk via SET ACCESS
-- METHOD. Add some uncompressed data to test migration with partially
-- compressed chunks.
select ch as alter_chunk from show_chunks('test2') ch limit 1 \gset
insert into :alter_chunk values ('2022-06-01 10:00', 4, 4, 4.0, 4.0);
alter table :alter_chunk set access method hypercore;

reset client_min_messages;

-- Check pg_am dependencies for the chunks. Since they are using heap
-- AM, there should be no dependencies as heap AM is always present.
select dep.objid::regclass, am.amname
from show_chunks('test2') ch
join pg_depend dep on (ch = dep.objid)
join pg_am am on (dep.refobjid = am.oid);

-- All chunks should use hypercore and have rel_size_stats
select * from compressed_rel_size_stats
where amname = 'hypercore' order by rel;

-- Check that query plan is now ColumnarScan and that all data, except
-- the one uncompressed row, is still compressed after migration
explain (costs off)
select _timescaledb_debug.is_compressed_tid(ctid) from test2
where not _timescaledb_debug.is_compressed_tid(ctid);

select _timescaledb_debug.is_compressed_tid(ctid) from test2
where not _timescaledb_debug.is_compressed_tid(ctid);

-- Check that the table still returns the correct count. Account for
-- the one uncompressed row inserted.
select count(*)=(:orig_test2_count + 1) as count_as_expected from test2;
commit;

\set ON_ERROR_STOP 0
-- Trying to convert a hypercore to a hypercore should be an error
-- if if_not_compressed is false and the hypercore is fully
-- compressed.
select compress_chunk(ch, compress_using => 'hypercore', if_not_compressed => false)
from show_chunks('test2') ch;

-- Compressing using something different than "hypercore" or "heap"
-- should not be allowed
select compress_chunk(ch, compress_using => 'non_existing_am')
from show_chunks('test2') ch;

\set ON_ERROR_STOP 1

-- Compressing from hypercore with compress_using=>heap should lead
-- to recompression of hypercore with a notice.
select compress_chunk(ch, compress_using => 'heap')
from show_chunks('test2') ch;

-- Compressing a hypercore without specifying compress_using should
-- lead to recompression. First check that :chunk is a hypercore.
select ch as chunk from show_chunks('test2') ch limit 1 \gset
select * from compressed_rel_size_stats
where amname = 'hypercore' and rel = :'chunk'::regclass;
insert into :chunk values ('2022-06-01 10:01', 6, 6, 6.0, 6.0);
select ctid from :chunk where created_at = '2022-06-01 10:01' and device_id = 6;
select compress_chunk(:'chunk');
select ctid from :chunk where created_at = '2022-06-01 10:01' and device_id = 6;
-- Compressing a hypercore with compress_using=>hypercore should
-- also lead to recompression
insert into :chunk values ('2022-06-01 11:02', 7, 7, 7.0, 7.0);
select ctid from :chunk where created_at = '2022-06-01 11:02' and device_id = 7;
select compress_chunk(:'chunk', compress_using => 'hypercore');
select ctid from :chunk where created_at = '2022-06-01 11:02' and device_id = 7;

-- Convert all hypercores back to heap
select decompress_chunk(rel) ch
  from compressed_rel_size_stats
  where amname = 'hypercore'
  order by ch;

-- Test that it is possible to convert multiple hypercores in the
-- same transaction. The goal is to check that all the state is
-- cleaned up between two or more commands in same transaction.
select ch as chunk2 from show_chunks('test2') ch offset 1 limit 1 \gset
start transaction;
select compress_chunk(:'chunk', compress_using => 'hypercore');
select compress_chunk(:'chunk2', compress_using => 'hypercore');
commit;

select * from compressed_rel_size_stats
where amname = 'hypercore' and relparent = 'test2'::regclass
order by rel;

-- Test that we can compress old way using compress_using=>heap
select ch as chunk3 from show_chunks('test2') ch offset 2 limit 1 \gset
select compress_chunk(:'chunk3', compress_using => 'heap');

select * from compressed_rel_size_stats
where amname = 'heap' and relparent = 'test2'::regclass
order by rel;

\set ON_ERROR_STOP 0
-- If we call compress_chunk with compress_using=>'heap' on a
-- heap-compressed chunk, it should lead to an error if
-- if_not_compressed is false. The commands below are all equivalent
-- in this case.
select compress_chunk(:'chunk3', compress_using => 'heap', if_not_compressed=>false);
select compress_chunk(:'chunk3', compress_using => NULL, if_not_compressed=>false);
select compress_chunk(:'chunk3', if_not_compressed=>false);
\set ON_ERROR_STOP 1

-- For a heap-compressed chunk, these should all be equivalent and
-- should not do anything when there is nothing to recompress. A
-- notice should be raised instead of an error.
select compress_chunk(:'chunk3', compress_using => 'heap');
select compress_chunk(:'chunk3', compress_using => NULL);
select compress_chunk(:'chunk3');

-- Insert new data to create a "partially compressed" chunk. Note that
-- it is not possible to insert directly into the chunk because that
-- doesn't properly update the partially compressed state.
insert into test2 values ('2022-06-15 16:00', 8, 8, 8.0, 8.0);
select * from only :chunk3;
select compress_chunk(:'chunk3', compress_using => 'heap');
-- The tuple should no longer be in the non-compressed chunk
select * from only :chunk3;
-- But the tuple is returned in a query without ONLY
select * from :chunk3 where created_at = '2022-06-15 16:00' and device_id = 8;

-- Test a more complicated schema from the NYC Taxi data set. This is
-- to test that compression using hypercore works, since there was an
-- issue with setting up the tuple sort state during compression.
create table rides (
    vendor_id text,
    pickup_datetime timestamptz not null,
    dropoff_datetime timestamptz not null,
    passenger_count numeric,
    trip_distance numeric,
    pickup_longitude numeric,
    pickup_latitude numeric,
    rate_code int,
    dropoff_longitude numeric,
    dropoff_latitude numeric,
    payment_type int,
    fare_amount numeric,
    extra numeric,
    mta_tax numeric,
    tip_amount numeric,
    tolls_amount numeric,
    improvement_surcharge numeric,
    total_amount numeric
);

select create_hypertable('rides', 'pickup_datetime', 'payment_type', 2, create_default_indexes=>false);
create index on rides (vendor_id, pickup_datetime desc);
create index on rides (pickup_datetime desc, vendor_id);
create index on rides (rate_code, pickup_datetime desc);
create index on rides (passenger_count, pickup_datetime desc);
alter table rides set (timescaledb.compress_segmentby='payment_type');

-- Insert some values. Particularly interested in testing text type handling.
insert into rides values
(745233436676,'2016-01-01 00:00:03','2016-01-01 00:11:14',1,6.00,-73.947151184082031,40.791046142578125,1,-73.920768737792969,40.865577697753906,2,9,0.5,0.5,0,0,0.3,19.3),
(6,'2016-01-01 00:00:02','2016-01-01 00:11:55',1,1.20,-73.979423522949219,40.744613647460938,1,-73.992034912109375,40.753944396972656,2,9,0.5,0.5,0,0,0.3,10.3),
(356,'2016-01-01 00:00:01','2016-01-01 00:11:55',1,1.20,-73.979423522949219,40.744613647460938,1,-73.992034912109375,40.753944396972656,2,9,0.5,0.5,0,0,0.3,10.3);
-- Check that it is possible to compress
select compress_chunk(ch, compress_using=>'hypercore') from show_chunks('rides') ch;
select rel, amname from compressed_rel_size_stats
where relparent::regclass = 'rides'::regclass;

-- Query to check everything is OK
analyze rides;

-- This should decompress and create text datums (column 1) in an
-- order that exercises datum caching in the arrow array
explain (costs off)
select * from rides order by pickup_datetime;
select * from rides order by pickup_datetime;
