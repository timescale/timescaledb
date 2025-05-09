-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE ACCESS METHOD testam TYPE TABLE HANDLER heap_tableam_handler;
set role :ROLE_DEFAULT_PERM_USER;

create view chunk_slices as
select
    h.table_name as hypertable_name,
    c.table_name as chunk_name,
    _timescaledb_functions.to_timestamp(ds.range_start) as range_start,
    _timescaledb_functions.to_timestamp(ds.range_end) as range_end
from _timescaledb_catalog.chunk c
join _timescaledb_catalog.chunk_constraint cc on (cc.chunk_id = c.id)
join _timescaledb_catalog.dimension_slice ds on (ds.id = cc.dimension_slice_id)
join _timescaledb_catalog.hypertable h on (h.id = c.hypertable_id)
order by range_start, range_end;


create table splitme (time timestamptz not null, device int, location int, temp float, comment text);
select create_hypertable('splitme', 'time', chunk_time_interval => interval '1 week');
alter table splitme set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='device');

--
-- Insert data to create two chunks with time ranges like this:
-- _____________
-- |     |     |
-- |  1  |  2  |
-- |_____|_____|
---
--- Make sure we have a long text value to create toast table
insert into splitme values
       ('2024-01-03 22:00', 1, 1, 1.0, 'foo'),
       ('2024-01-09 15:00', 1, 2, 2.0, 'barbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbarbar');

-- Remove a column to ensure that split can handle it
alter table splitme drop column location;

-- All data in single chunk
select chunk_name, range_start, range_end
from timescaledb_information.chunks
order by chunk_name, range_start, range_end;
select time, device, temp from _timescaledb_internal._hyper_1_1_chunk order by time;
select * from chunk_slices where hypertable_name = 'splitme';

\set ON_ERROR_STOP 0
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', split_at => 1);
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', split_at => 1::int);
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', split_at => '2024-01-04 00:00'::timestamp);

-- Split at start of chunk range
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', split_at => 'Wed Jan 03 16:00:00 2024 PST');
-- Split at end of chunk range
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', split_at => 'Wed Jan 10 16:00:00 2024 PST');
-- Split at multiple points. Not supported yet.
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', split_at => '{ 2024-01-04 10:00, 2024-01-07 12:00 }'::timestamptz[]);
-- Try to split something which is not a chunk
call split_chunk('splitme');
-- Split a chunk with unsupported access method
alter table _timescaledb_internal._hyper_1_1_chunk set access method testam;
call split_chunk('_timescaledb_internal._hyper_1_1_chunk');
alter table _timescaledb_internal._hyper_1_1_chunk set access method heap;

-- Split an OSM chunk is not supported
reset role;
update _timescaledb_catalog.chunk ch set osm_chunk = true where table_name = '_hyper_1_1_chunk';
set role :ROLE_DEFAULT_PERM_USER;

call split_chunk('_timescaledb_internal._hyper_1_1_chunk');
reset role;
update _timescaledb_catalog.chunk ch set osm_chunk = false where table_name = '_hyper_1_1_chunk';
set role :ROLE_DEFAULT_PERM_USER;

-- Split a frozen chunk is not supported
select _timescaledb_functions.freeze_chunk('_timescaledb_internal._hyper_1_1_chunk');
call split_chunk('_timescaledb_internal._hyper_1_1_chunk');
select _timescaledb_functions.unfreeze_chunk('_timescaledb_internal._hyper_1_1_chunk');

-- Split by non-owner is not allowed
set role :ROLE_1;
call split_chunk('_timescaledb_internal._hyper_1_1_chunk');
set role :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 1

call split_chunk('_timescaledb_internal._hyper_1_1_chunk', split_at => '2024-01-04 00:00');

select chunk_name, range_start, range_end
from timescaledb_information.chunks
order by chunk_name, range_start, range_end;
select * from chunk_slices where hypertable_name = 'splitme';

-- Show that the two tuples ended up in different chunks
select time, device, temp from _timescaledb_internal._hyper_1_1_chunk order by time;
select time, device, temp from _timescaledb_internal._hyper_1_2_chunk order by time;

select setseed(0.2);
-- Test split with bigger data set and chunks with more blocks
insert into splitme (time, device, temp)
select t, ceil(random()*10), random()*40
from generate_series('2024-01-03 23:00'::timestamptz, '2024-01-10 01:00', '10s') t;
select count(*) from splitme;

-- Add back location just to make things more difficult
alter table splitme add column location int default 1;

-- There are two space partitions (device), so several chunks will
-- have the same time ranges
select chunk_name, range_start, range_end
from timescaledb_information.chunks
order by chunk_name, range_start, range_end;

-- Split chunk 2. Save count to compare after split.
select count(*) from _timescaledb_internal._hyper_1_2_chunk;
select count(*) orig_count from _timescaledb_internal._hyper_1_2_chunk \gset

-- Generate some garbage so that we can see that it gets cleaned up
-- during split
update  _timescaledb_internal._hyper_1_2_chunk set temp = temp+1 where temp > 10;

-- This will split in two equal size chunks
call split_chunk('_timescaledb_internal._hyper_1_2_chunk');

select chunk_name, range_start, range_end
from timescaledb_information.chunks
order by chunk_name, range_start, range_end;

-- Check that the counts in the two result partitions is the same as
-- in the original partition and that the tuple counts are roughly the
-- same across the partitions.
with counts as (
    select (select count(*) from _timescaledb_internal._hyper_1_2_chunk) count1,
            (select count(*) from _timescaledb_internal._hyper_1_3_chunk) count2
) select
  c.count1, c.count2,
  c.count1 + c.count2 as total_count,
  (c.count1 + c.count2) = :orig_count as is_same_count
from counts c;

-- Check that both rels return proper data and no columns are messed
-- up
select time, device, location, temp from _timescaledb_internal._hyper_1_2_chunk order by time, device limit 3;
select time, device, location, temp from _timescaledb_internal._hyper_1_3_chunk order by time, device limit 3;

--
-- Test split with integer time
--
create table splitme_int (time int not null, device int, temp float);
select create_hypertable('splitme_int', 'time', chunk_time_interval => 10::int);

insert into splitme_int values (1, 1, 1.0), (8, 8, 8.0);
select ch as int_chunk from show_chunks('splitme_int') ch order by ch limit 1 \gset

select * from chunk_slices where hypertable_name = 'splitme_int';

\set ON_ERROR_STOP 0
call split_chunk(:'int_chunk', split_at => 0);
call split_chunk(:'int_chunk', split_at => 10);
\set ON_ERROR_STOP 1

call split_chunk(:'int_chunk', split_at => '5');

select * from chunk_slices where hypertable_name = 'splitme_int';

select * from :int_chunk order by time;
select * from splitme_int order by time;

-- Split with one empty chunk
call split_chunk(:'int_chunk', split_at => 3);
select * from chunk_slices where hypertable_name = 'splitme_int';

select * from :int_chunk order by time;
select ch as int_chunk from show_chunks('splitme_int') ch order by ch limit 1 offset 2 \gset
\echo :int_chunk
select * from :int_chunk order by time;
-- Insert data into the empty chunk
insert into splitme_int values (4, 4, 4.0);
select * from :int_chunk order by time;


--
-- Try with more data after split
--

create view chunk_info as
select relname as chunk, amname as tam, con.conname, pg_get_expr(conbin, ch) checkconstraint
from pg_class cl
join pg_am am on (cl.relam = am.oid)
join show_chunks('splitme') ch on (cl.oid = ch)
join pg_constraint con on (con.conrelid = ch)
where con.contype = 'c'
order by 1,2,3 desc;

-- Remove comment column to generate dropped column
alter table splitme drop column comment;

select * from chunk_info;
\c :TEST_DBNAME :ROLE_SUPERUSER
set role :ROLE_DEFAULT_PERM_USER;

select * from chunk_slices where hypertable_name = 'splitme';

insert into splitme (time, device, location, temp)
select t, ceil(random()*10), ceil(random()*20), random()*40
from generate_series('2024-01-03'::timestamptz, '2024-01-10', '10s') t;

select * from chunk_info;
call split_chunk('_timescaledb_internal._hyper_1_2_chunk');
select * from chunk_info;

--
-- Test multi-dimensional hypertable
--
-- Currently not supported because the subspace cache cannot handle
-- tuple routing when there are two overlapping primary dimension
-- ranges. This can happen when the "time" range is split in one space
-- partition but not the other.
--
create table splitme_md (time timestamptz not null, device int, location int, temp float);
select create_hypertable('splitme_md', 'time', 'device', 2, chunk_time_interval => interval '1 week');
insert into splitme_md values
       ('2024-01-03 22:00', 1, 1, 1.0),
       ('2024-01-09 15:00', 1, 2, 2.0);

select ch as chunk_md from show_chunks('splitme_md') ch limit 1 \gset
select * from chunk_slices where hypertable_name = 'splitme_md';
\set ON_ERROR_STOP 0
-- Currently can't split multi-dimensional chunks due to bug/limitation in subspace store.
call split_chunk(:'chunk_md');
\set ON_ERROR_STOP 1

-- Split when insert in progress
begin;
insert into splitme values ('2024-01-04 22:00', 20, 20, 20.0);
call split_chunk('_timescaledb_internal._hyper_1_1_chunk');
rollback;

-- Split when delete in progress
begin;
delete from splitme where device = 1;
call split_chunk('_timescaledb_internal._hyper_1_1_chunk');
rollback;


--- Split a compressed/columnstore chunk is not supported
select * from _timescaledb_catalog.compression_settings;

call convert_to_columnstore('_timescaledb_internal._hyper_1_1_chunk');
select compress_relid from _timescaledb_catalog.compression_settings where relid = '_timescaledb_internal._hyper_1_1_chunk'::regclass \gset

\d+ :compress_relid
select * from :compress_relid;


select * from chunk_slices where hypertable_name = 'splitme';
select count(*), sum(device), sum(location), sum(temp) from splitme;
select count(*), sum(device), sum(location), sum(temp) from _timescaledb_internal._hyper_1_1_chunk;

call split_chunk('_timescaledb_internal._hyper_1_1_chunk');


select * from chunk_slices where hypertable_name = 'splitme';
select count(*), sum(device), sum(location), sum(temp) from splitme;
select count(*), sum(device), sum(location), sum(temp) from _timescaledb_internal._hyper_1_1_chunk;
select count(*), sum(device), sum(location), sum(temp) from _timescaledb_internal._hyper_1_13_chunk;

select * from timescaledb_information.chunks where chunk_name = '_hyper_1_13_chunk';
select compress_relid from _timescaledb_catalog.compression_settings where relid = '_timescaledb_internal._hyper_1_13_chunk'::regclass \gset

\d+ :compress_relid
select * from :compress_relid;
