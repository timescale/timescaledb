-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
create extension pgstattuple;
create extension pageinspect;
set role :ROLE_DEFAULT_PERM_USER;
select setseed(0.3);

-- Create view to see the compressed relations
create view compressed_rels as
with reg_chunk as (
     select * from _timescaledb_catalog.chunk where compressed_chunk_id IS NOT NULL
)
select format('%I.%I', reg_chunk.schema_name, reg_chunk.table_name)::regclass as relid,
       format('%I.%I', cpr_chunk.schema_name, cpr_chunk.table_name)::regclass as compressed_relid
from _timescaledb_catalog.chunk cpr_chunk
inner join reg_chunk on (cpr_chunk.id = reg_chunk.compressed_chunk_id);

-- Create two hypertables with same config and data, apart from one
-- having a hyperstore chunk (hystable). The regular table (regtable)
-- will be used as a reference.
create table hystable(time timestamptz, location bigint, device smallint, temp float4);
create table regtable(time timestamptz, location bigint, device smallint, temp float4);
select create_hypertable('hystable', 'time', create_default_indexes => false);
select create_hypertable('regtable', 'time', create_default_indexes => false);

insert into regtable (time, location, device, temp)
values ('2022-06-01 00:01', 1, 1, 1.0),
       ('2022-06-01 00:02', 2, 2, 2.0),
       ('2022-06-01 00:03', 1, 3, 3.0),
       ('2022-06-01 00:04', 2, 3, 4.0);

insert into hystable select * from regtable;

-- Make sure new chunks are hyperstore from the start, except
-- obviously for the chunk that was already created.
alter table hystable set access method hyperstore, set (
      timescaledb.compress_orderby = 'time',
      timescaledb.compress_segmentby = 'location'
);

select ch chunk, amname access_method
from show_chunks('hystable') ch
inner join pg_class cl on (cl.oid = ch)
inner join pg_am am on (cl.relam = am.oid);

select format('%I.%I', chunk_schema, chunk_name)::regclass as hystable_chunk
  from timescaledb_information.chunks
 where format('%I.%I', hypertable_schema, hypertable_name)::regclass = 'hystable'::regclass
 order by hystable_chunk asc
 limit 1 \gset

select format('%I.%I', chunk_schema, chunk_name)::regclass as regtable_chunk
  from timescaledb_information.chunks
 where format('%I.%I', hypertable_schema, hypertable_name)::regclass = 'regtable'::regclass
 order by regtable_chunk asc
 limit 1 \gset

-- create some indexes for testing
create index hystable_device_idx on hystable (device);
create index regtable_device_idx on regtable (device);

-- create more indexes to test vacuum on multiple indexes as well as
-- on a segmentby column
create index hystable_temp_idx on hystable (temp);

-- The location index on "hystable" is special since it is a segmentby
-- index. Check that vacuum works on this index too.
create index hystable_location_idx on hystable (location);

-- Also create a multi-column index, which includes segmentby
create index hystable_time_location_idx on hystable (time, location);

select indexrelid::regclass as hystable_device_chunk_idx
from pg_index i inner join pg_class c on (i.indexrelid=c.oid)
where indrelid = :'hystable_chunk'::regclass
and relname like '%device%' \gset

select indexrelid::regclass as regtable_device_chunk_idx
from pg_index i inner join pg_class c on (i.indexrelid=c.oid)
where indrelid = :'regtable_chunk'::regclass
and relname like '%device%' \gset

select indexrelid::regclass as hystable_temp_chunk_idx
from pg_index i inner join pg_class c on (i.indexrelid=c.oid)
where indrelid = :'hystable_chunk'::regclass
and relname like '%temp%' \gset

select indexrelid::regclass as hystable_location_chunk_idx
from pg_index i inner join pg_class c on (i.indexrelid=c.oid)
where indrelid = :'hystable_chunk'::regclass
and relname like '%hystable_location%' \gset

alter table :hystable_chunk set access method hyperstore;

-- Show new access method on chunk
select ch chunk, amname access_method
from show_chunks('hystable') ch
inner join pg_class cl on (cl.oid = ch)
inner join pg_am am on (cl.relam = am.oid);

-- Reset to superuser in order to run bt_page_items()
reset role;

select * from bt_page_items(:'hystable_device_chunk_idx', 1);
select * from bt_page_items(:'regtable_device_chunk_idx', 1);

-- The "temp" index should have four entries because all values are
-- unique
select * from bt_page_items(:'hystable_temp_chunk_idx', 1);

-- The "location" index is a segmentby index and only points to whole
-- compressed tuples (not individual values in the compressed
-- tuple). So, it should only have pointers corresponding to the two
-- segments
select * from bt_page_items(:'hystable_location_chunk_idx', 1);

-- A delete on "device" will decompress all the data and thus insert
-- four new tuples into the non-compressed relation that are also
-- added to the index. Note that the values to be deleted are first
-- inserted before being deleted, further inflating index size with
-- old tuple versions of same data.
delete from hystable where device=1;
delete from regtable where device=1;

-- Show inflated size of indexes, especially compared to reference
-- table.
select * from bt_page_items(:'hystable_device_chunk_idx', 1);
select * from bt_page_items(:'regtable_device_chunk_idx', 1);

-- Show garbage also in other indexes
select * from bt_page_items(:'hystable_temp_chunk_idx', 1);

select * from bt_page_items(:'hystable_location_chunk_idx', 1);

-- Run vacuum to clean up dead tuples
vacuum (index_cleanup on) hystable;

-- Only index entries pointing to non-compressed tuples should remain
select * from bt_page_items(:'hystable_device_chunk_idx', 1);
select * from bt_page_items(:'hystable_temp_chunk_idx', 1);
select * from bt_page_items(:'hystable_location_chunk_idx', 1);

-- insert the deleted data again
insert into hystable (time, location, device, temp)
values ('2022-06-01 00:01', 1, 1, 1.0);

-- Recompress to get data back into compressed form
select compress_chunk(:'hystable_chunk');

-- Index is again inflated with entries to both old (dead) entries in
-- non-compressed rel and new entries in the compressed rel.
select * from bt_page_items(:'hystable_device_chunk_idx', 1);

-- Run vacuum again to clean up garbage from recompression
vacuum (index_cleanup on) hystable;
select * from bt_page_items(:'hystable_device_chunk_idx', 1);
select * from bt_page_items(:'hystable_temp_chunk_idx', 1);
select * from bt_page_items(:'hystable_location_chunk_idx', 1);

-- Insert bigger data set and create new chunks
insert into regtable (time, location, device, temp)
select t, ceil(random()*10), ceil(random()*30), random()*40
from generate_series('2022-06-01'::timestamptz, '2022-06-10', '60s') t;

insert into hystable select * from regtable;

-- All new chunks should be hyperstores since we configured hyperstore
-- as default hypertable AM
select ch, amname
from show_chunks('hystable') ch
inner join pg_class cl on (cl.oid = ch)
inner join pg_am am on (cl.relam = am.oid);

-- All (new) compressed chunks should have a hsproxy index
select indexrelid::regclass
from pg_index i inner join
compressed_rels crels on (i.indrelid = crels.compressed_relid);

select tuple_count from pgstattuple(:'hystable_device_chunk_idx');

-- Compressing will move all the data to the compressed relation and
-- rebuild indexes. It will optimize the structure of the index and
-- thus reduce the number of index tuples
select compress_chunk(:'hystable_chunk');
select tuple_count from pgstattuple(:'hystable_device_chunk_idx');

-- Test vacuum without index cleanup. Should not remove dead tuples
-- from indexes according to pgstattuple count.
vacuum (index_cleanup off) hystable;
select tuple_count from pgstattuple(:'hystable_device_chunk_idx');

-- Running vacuum with index_cleanup on should further reduce the
-- number of index tuples by removing pointers to dead heap tuples
-- (old uncompressed tuples).
vacuum (index_cleanup on) hystable;
select tuple_count from pgstattuple(:'hystable_device_chunk_idx');

-- Check that index scans return correct count()
select count(*) from regtable where temp > 10;
select count(*) from hystable where temp > 10;

select count(*) from regtable where location > 4;
select count(*) from hystable where location > 4;

select count(*) from regtable where device > 5;
select count(*) from hystable where device > 5;

drop table hystable;

-- Test vacuum on a table that doesn't have any indexes
create table hystable(time timestamptz, location int, device int, temp float);
select create_hypertable('hystable', 'time', create_default_indexes => false);

-- This time create the table without a segmentby column
alter table hystable set access method hyperstore, set (
      timescaledb.compress_orderby = 'time'
);

-- vacuum on empty table
vacuum (index_cleanup on) hystable;

insert into hystable select * from regtable;

-- All chunks should be hyperstores
select ch, amname
from show_chunks('hystable') ch
inner join pg_class cl on (cl.oid = ch)
inner join pg_am am on (cl.relam = am.oid);

-- All compressed chunks should have a hsproxy index
select indexrelid::regclass
from pg_index i inner join
compressed_rels crels on (i.indrelid = crels.compressed_relid);

-- delete some data to generate garbage
delete from hystable where temp > 20;
vacuum (index_cleanup on) hystable;
