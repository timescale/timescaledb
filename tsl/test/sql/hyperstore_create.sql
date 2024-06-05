-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/hyperstore_helpers.sql
select setseed(0.3);

-- Testing the basic API for creating a hyperstore
CREATE TABLE test2(
	   created_at timestamptz not null,
	   location_id int,
	   device_id int,
	   temp float,
	   humidity float
);

create index on test2(device_id, created_at);

\set ON_ERROR_STOP 0
alter table test2 set access method hyperstore;
\set ON_ERROR_STOP 1

select create_hypertable('test2', 'created_at');

\set ON_ERROR_STOP 0
-- Should show error since there is no namespace.
alter table test2
	  set access method hyperstore,
	  set (compress_segmentby = 'location_id');
\set ON_ERROR_STOP 1

alter table test2
	  set access method hyperstore,
	  set (timescaledb.compress_segmentby = 'location_id');

-- Test altering hypertable to hyperstore again. It should be allowed
-- and be a no-op.
alter table test2 set access method hyperstore;

\set ON_ERROR_STOP 0
-- This shows an error but the error is weird, we should probably get
-- a better one.
alter table test2
	  set access method hyperstore,
	  set (compress_segmentby = 'location_id');
\set ON_ERROR_STOP 1

-- Create view for hyperstore rels
create view amrels as
select cl.oid::regclass as rel, am.amname, inh.inhparent::regclass as relparent
  from pg_class cl
  inner join pg_am am on (cl.relam = am.oid)
  left join pg_inherits inh on (inh.inhrelid = cl.oid);

-- Show that test2 is a hyperstore
select rel, amname
from amrels
where rel='test2'::regclass;

-- This will create new chunks for the hypertable
insert into test2 (created_at, location_id, device_id, temp, humidity)
select t, ceil(random()*10), ceil(random()*30), random()*40, random()*100
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5s') t;

-- All chunks should use the hyperstore access method
select * from amrels
where relparent='test2'::regclass;

-- Show compression settings for hyperstore across catalog and views
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

-- Check that chunk is NOT using hyperstore
select rel, amname
from amrels
where relparent='test3'::regclass;

\set ON_ERROR_STOP 0
alter table :chunk set access method hyperstore;
\set ON_ERROR_STOP 1

-- Add compression settings
alter table test3 set (timescaledb.compress_segmentby = 'device');
alter table :chunk set access method hyperstore;

-- Check that chunk is using hyperstore
select * from amrels where rel=:'chunk'::regclass;

-- Test setting same access method again
alter table :chunk set access method hyperstore;

-- Create a second chunk
insert into test3 values ('2022-08-01', 1, 1.0);

-- The second chunk should not be a hyperstore chunk

select * from amrels where relparent='test3'::regclass;

-- Set hyperstore on hypertable
alter table test3 set access method hyperstore;

-- Create a third chunk
insert into test3 values ('2022-10-01', 1, 1.0);

-- The third chunk should be a hyperstore chunk
select * from amrels where relparent='test3'::regclass;

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

-- There should be no hyperstore chunks that lack compression size stats
select count(*) as num_stats_missing from compressed_rel_size_stats
where amname = 'hyperstore' and numrows_pre_compression is null;

-- Show stats for hyperstore chunks. Note that many stats are 0 since
-- chunks were created as a result of inserts and not really
-- compressed
select * from compressed_rel_size_stats order by rel;

-- Decompress hyperstores to check that stats are removed
select untwist_chunk(rel)
  from compressed_rel_size_stats
  where amname = 'hyperstore';

-- All stats should be removed
select count(*) as orphaned_stats
from compressed_rel_size_stats;

-- Create hyperstores again and check that compression size stats are
-- updated showing compressed data
select twist_chunk(ch)
from show_chunks('test2') ch;
select twist_chunk(ch)
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
select compress_chunk(untwist_chunk(ch))
from show_chunks('test2') ch;
select compress_chunk(untwist_chunk(ch))
from show_chunks('test3') ch;

select * from compressed_rel_size_stats order by rel;

-- Check that stats are the same for hyperstore and now with
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
