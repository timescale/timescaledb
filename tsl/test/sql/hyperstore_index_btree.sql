-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
create extension pageinspect;
set role :ROLE_DEFAULT_PERM_USER;

\ir include/setup_hyperstore.sql

-- Avoid parallel (index) scans to make test stable
set max_parallel_workers_per_gather to 0;

-- Drop the device_id index and redefine it later with extra columns.
drop index hypertable_device_id_idx;

create view chunk_indexes as
select ch::regclass::text as chunk, indexrelid::regclass::text as index, attname
from pg_attribute att inner join pg_index ind
on (att.attrelid=ind.indrelid and att.attnum=ind.indkey[0])
inner join show_chunks(:'hypertable') ch on (ch = att.attrelid)
order by chunk, index;

-- To get stable plans
set max_parallel_workers_per_gather to 0;

-- save some reference data from an index (only) scan
select explain_anonymize(format($$
       select location_id, count(*) into orig from %s
       where location_id in (3,4,5) group by location_id
$$, :'hypertable'));

select location_id, count(*) into orig from :hypertable
where location_id in (3,4,5) group by location_id;

-- Create other segmentby indexes to test different combinations. Also
-- redefine the device_id index to include one value field in the
-- index and check that index-only scans work also for included
-- attributes.
create index hypertable_location_id_include_humidity_idx on :hypertable (location_id) include (humidity);
create index hypertable_device_id_idx on :hypertable (device_id) include (humidity);
create index hypertable_owner_idx on :hypertable (owner_id);
create index hypertable_location_id_owner_id_idx on :hypertable (location_id, owner_id);

-- Save index size before switching to hyperstore so that we can
-- compare sizes after. Don't show the actual sizes because it varies
-- slightly on different platforms.
create table index_sizes_before as
select index, pg_relation_size(index::regclass)
from chunk_indexes
where chunk::regclass = :'chunk2'::regclass
and (attname='location_id' or attname='device_id' or attname='owner_id');

-- Drop some segmentby indexes and recreate them after converting to
-- hyperstore. This is to test having some created before conversion
-- and some after.
drop index hypertable_owner_idx;
drop index hypertable_location_id_owner_id_idx;

alter table :chunk2 set access method hyperstore;

-- count without indexes
select owner_id, count(*) into owner_orig from :hypertable
where owner_id in (3,4,5) group by owner_id;

-- create indexes on all segmentby columns
create index hypertable_owner_idx on :hypertable (owner_id);
create index hypertable_location_id_owner_id_idx on :hypertable (location_id, owner_id);

-- Result should be the same with indexes
select explain_anonymize(format($$
       select owner_id, count(*) into owner_comp from %s
       where owner_id in (3,4,5) group by owner_id
$$, :'hypertable'));
select owner_id, count(*) into owner_comp from :hypertable
where owner_id in (3,4,5) group by owner_id;
select * from owner_orig join owner_comp using (owner_id) where owner_orig.count != owner_comp.count;

-- the indexes on segmentby columns should be smaller on hyperstore,
-- except for the covering index on location_id (because it also
-- includes the non-segmentby column humidity).  The device_id index
-- should also remain the same size since it is not on a segmentby
-- column.
select
    a.index,
    pg_relation_size(a.index) = b.pg_relation_size as is_same_size,
    pg_relation_size(a.index) < b.pg_relation_size as is_smaller
from chunk_indexes a
join index_sizes_before b on (a.index = b.index)
where chunk::regclass=:'chunk2'::regclass
and (attname='location_id' or attname='device_id' or attname='owner_id');

-- the query should not use index-only scan on the hypestore chunk
-- (number 2) because it is not supported on segmentby indexes
select explain_anonymize(format($$
       select location_id, count(*) into comp from %s
       where location_id in (3,4,5) group by location_id
$$, :'hypertable'));

-- result should be the same
select location_id, count(*) into comp from :hypertable where location_id in (3,4,5) group by location_id;
select * from orig join comp using (location_id) where orig.count != comp.count;

drop table orig, owner_orig, owner_comp;

--
-- test that indexes work after updates
--
select _timescaledb_debug.is_compressed_tid(ctid), created_at, location_id, temp
from :chunk2 order by location_id, created_at desc limit 2;

-- first update moves the value from the compressed rel to the non-compressed (seen via ctid)
update :hypertable set temp=1.0 where location_id=1 and created_at='Wed Jun 08 16:57:50 2022 PDT';
select  _timescaledb_debug.is_compressed_tid(ctid), created_at, location_id, temp
from :chunk2 order by location_id, created_at desc limit 2;

-- second update should be a hot update (tuple in same block after update, as shown by ctid)
update :hypertable set temp=2.0 where location_id=1 and created_at='Wed Jun 08 16:57:50 2022 PDT';
select  _timescaledb_debug.is_compressed_tid(ctid), created_at, location_id, temp
from :chunk2 order by location_id, created_at desc limit 2;

-- make sure query uses a segmentby index and returns the correct data for the update value
select explain_anonymize(format($$
       select created_at, location_id, temp from %s where location_id=1 and temp=2.0
$$, :'chunk2'));
select created_at, location_id, temp from :chunk2 where location_id=1 and temp=2.0;

select compress_chunk(show_chunks(:'hypertable'), compress_using => 'hyperstore');

vacuum analyze :hypertable;

-- Test sequence scan
set enable_indexscan to off;
select explain_analyze_anonymize(format('select * from %s where owner_id = 3', :'hypertable'));

-- TODO(timescale/timescaledb-private#1117): the Decompress Count here
-- is not correct, but the result shows correctly.
select explain_analyze_anonymize(format('select * from %s where owner_id = 3', :'chunk1'));
reset enable_indexscan;

-- Test index scan on non-segmentby column
select explain_analyze_anonymize(format($$
   select device_id, avg(temp) from %s where device_id between 10 and 20
   group by device_id
$$, :'hypertable'));

select explain_analyze_anonymize(format($$
    select device_id, avg(temp) from %s where device_id between 10 and 20
    group by device_id
$$, :'chunk1'));

-- Test index scan on segmentby column
select explain_analyze_anonymize(format($$
    select created_at, location_id, temp from %s where location_id between 5 and 10
$$, :'hypertable'));

select explain_analyze_anonymize(format($$
    select created_at, location_id, temp from %s where location_id between 5 and 10
$$, :'chunk1'));

-- These should generate decompressions as above, but for all columns.
select explain_analyze_anonymize(format($$
    select * from %s where location_id between 5 and 10
$$, :'hypertable'));

select explain_analyze_anonymize(format($$
    select * from %s where location_id between 5 and 10
$$, :'chunk1'));

--
-- Test index only scan
--

create table saved_hypertable as select * from :hypertable;

-- This will not use index-only scan because it is using a segment-by
-- column, but we check that it works as expected though.
--
-- Note that the number of columns decompressed should be zero, since
-- we do not have to decompress any columns.
select explain_analyze_anonymize(format($$
    select location_id from %s where location_id between 5 and 10
$$, :'hypertable'));

-- We just compare the counts here, not the full content.
select heapam.count as heapam, hyperstore.count as hyperstore
  from (select count(location_id) from :hypertable where location_id between 5 and 10) heapam,
       (select count(location_id) from :hypertable where location_id between 5 and 10) hyperstore;

drop table saved_hypertable;

\echo == This should use index-only scan ==
select explain_analyze_anonymize(format($$
    select device_id from %s where device_id between 5 and 10
$$, :'hypertable'));

select explain_analyze_anonymize(format($$
    select location_id from %s where location_id between 5 and 10
$$, :'chunk1'));
select explain_analyze_anonymize(format($$
    select device_id from %s where device_id between 5 and 10
$$, :'chunk1'));

-- Test index only scan with covering indexes
select explain_analyze_anonymize(format($$
    select location_id, avg(humidity) from %s where location_id between 5 and 10
    group by location_id order by location_id
$$, :'hypertable'));

select explain_analyze_anonymize(format($$
    select device_id, avg(humidity) from %s where device_id between 5 and 10
    group by device_id order by device_id
$$, :'hypertable'));

select explain_analyze_anonymize(format($$
    select location_id, avg(humidity) from %s where location_id between 5 and 10
    group by location_id order by location_id
$$, :'chunk1'));

select explain_analyze_anonymize(format($$
    select device_id, avg(humidity) from %s where device_id between 5 and 10
    group by device_id order by device_id
$$, :'chunk1'));

select location_id, round(avg(humidity)) from :hypertable where location_id between 5 and 10
group by location_id order by location_id;

select location_id, round(avg(humidity)) from :chunk1 where location_id between 5 and 10
group by location_id order by location_id;

select device_id, round(avg(humidity)) from :hypertable where device_id between 5 and 10
group by device_id order by device_id;

select device_id, round(avg(humidity)) from :chunk1 where device_id between 5 and 10
group by device_id order by device_id;

-------------------------------------
-- Test UNIQUE and Partial indexes --
-------------------------------------
\set VERBOSITY default

---
-- Test that building a UNIQUE index won't work on a hyperstore table
-- that contains non-unique values.
---
create table non_unique_metrics (time timestamptz, temp float, device int);
select create_hypertable('non_unique_metrics', 'time', create_default_indexes => false);
insert into non_unique_metrics values ('2024-01-01', 1.0, 1), ('2024-01-01', 2.0, 1), ('2024-01-02', 3.0, 2);
select ch as non_unique_chunk from show_chunks('non_unique_metrics') ch limit 1 \gset
alter table non_unique_metrics set (timescaledb.compress_segmentby = 'device', timescaledb.compress_orderby = 'time');
alter table :non_unique_chunk set access method hyperstore;

\set ON_ERROR_STOP 0
---
-- UNIQUE index creation on compressed hyperstore should fail due to
-- non-unique values
---
create unique index on non_unique_metrics (time);
\set ON_ERROR_STOP 1

--------------------------
-- Test partial indexes --
--------------------------

-- Create partial predicate index
create index time_idx on non_unique_metrics (time) where (time < '2024-01-02'::timestamptz);

-- Find the index on the chunk and save as a variable
select indexrelid::regclass as chunk_time_idx
from pg_index i inner join pg_class c on (i.indexrelid=c.oid)
where indrelid = :'non_unique_chunk'::regclass
and relname like '%time%' \gset

reset role; -- need superuser for pageinspect

-- The index should contain 1 key (device 1) with 2 TIDs
select _timescaledb_debug.is_compressed_tid(ctid), dead, htid, tids from bt_page_items(:'chunk_time_idx', 1);

-- Check that a query can use the index
explain (costs off)
select * from non_unique_metrics where time <= '2024-01-01'::timestamptz;
select * from non_unique_metrics where time <= '2024-01-01'::timestamptz;

-- Test a partial index with a predicate on non-index column
drop index time_idx;
create index time_idx on non_unique_metrics (time) where (device < 2);

select indexrelid::regclass as chunk_time_idx
from pg_index i inner join pg_class c on (i.indexrelid=c.oid)
where indrelid = :'non_unique_chunk'::regclass
and relname like '%time%' \gset

-- Index should have two tids, since one row excluded
select _timescaledb_debug.is_compressed_tid(ctid), dead, htid, tids from bt_page_items(:'chunk_time_idx', 1);

-- Check that the index works. Expect two rows to be returned.
explain (costs off)
select * from non_unique_metrics where time < '2024-01-02'::timestamptz and device < 2;
select * from non_unique_metrics where time < '2024-01-02'::timestamptz and device < 2;

drop index time_idx;
create index time_idx on non_unique_metrics (time) where (device < 2) and temp < 2.0;

select indexrelid::regclass as chunk_time_idx
from pg_index i inner join pg_class c on (i.indexrelid=c.oid)
where indrelid = :'non_unique_chunk'::regclass
and relname like '%time%' \gset

-- Index should have two tids, since one row excluded
select _timescaledb_debug.is_compressed_tid(ctid), dead, htid, tids from bt_page_items(:'chunk_time_idx', 1);

-- Check that the index works as expected. Only one row should match.
explain (costs off)
select * from non_unique_metrics where time < '2024-01-02'::timestamptz and device < 2 and temp = 1.0;
select * from non_unique_metrics where time < '2024-01-02'::timestamptz and device < 2 and temp = 1.0;

-- Make time column data unique to test UNIQUE constraint/index creation
delete from non_unique_metrics where temp = 1.0;
alter table non_unique_metrics add constraint u1 unique(time);
\set ON_ERROR_STOP 0
insert into non_unique_metrics values ('2024-01-01', 1.0, 1);
\set ON_ERROR_STOP 1
-- Should also be able to create via "create index"
create unique index ui1 on non_unique_metrics (time);

drop table :hypertable cascade;
