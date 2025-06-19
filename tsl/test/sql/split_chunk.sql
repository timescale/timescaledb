-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE ACCESS METHOD testam TYPE TABLE HANDLER heap_tableam_handler;
set role :ROLE_DEFAULT_PERM_USER;

-- Test utility wrapper around split_chunk() to validate the split. It
-- automatically performs some basic checks to validate that the split
-- is OK.
create or replace procedure split_chunk_validate(chunk regclass, split_at timestamptz = null) as $$
declare
    chunk_stats_before _timescaledb_catalog.compression_chunk_size;
    uncompressed_heap_size_sum bigint;
    compressed_heap_size_sum bigint;
    hypertable regclass;
    new_chunk regclass;
    children regclass[];
    stats_are_equal boolean;
    count_before bigint;
    count_after1 bigint;
    count_after2 bigint;
    chunk_reltuples float8;
    new_chunk_reltuples float8;
begin
    -- Get the chunk size stats if compressed or it will be null
    select * into chunk_stats_before
    from _timescaledb_catalog.compression_chunk_size ccs
    join _timescaledb_catalog.chunk c on (c.id = ccs.chunk_id)
    join pg_class cl on (cl.relname = c.table_name)
    join pg_namespace ns on (ns.oid = cl.relnamespace and c.schema_name = ns.nspname)
    where cl.oid = chunk;

    -- Compare chunk children before and after split to get new chunk
    select inhparent into hypertable
    from pg_inherits
    where inhrelid = chunk;

    select array_agg(inhrelid) into children
    from pg_inherits
    where inhparent = hypertable;

    execute format('select count(*) from %s ch', chunk) into count_before;

    call split_chunk(chunk, split_at);

    with arr_diff as (
         select inhrelid as rel
         from pg_inherits
         where inhparent = hypertable
         except
         select unnest(children)
    ) select rel into new_chunk from arr_diff;


    -- Add up the counts for the two result chunks. Would normally do
    -- a UNION ALL query across the two chunks but it doesn't work for
    -- compressed chunks.
    execute format('select count(*) from %s ch', chunk) into count_after1;
    execute format('select count(*) from %s ch', new_chunk) into count_after2;

    if count_before != (count_after1 + count_after2) then
       raise exception 'count before split is different from count after: % vs % (% + %)',
             count_before, count_after1 + count_after2, count_after1, count_after2;
    end if;

    execute format('select reltuples from pg_class where oid=%s', chunk::oid) into chunk_reltuples;
    execute format('select reltuples from pg_class where oid=%s', new_chunk::oid) into new_chunk_reltuples;

    raise notice 'chunks after split are % (reltuples %) and % (reltuples %)',
          chunk, chunk_reltuples, new_chunk, new_chunk_reltuples;

    if chunk_stats_before is not null then
        select
            sum(uncompressed_heap_size), sum(compressed_heap_size)
            into uncompressed_heap_size_sum, compressed_heap_size_sum
            from _timescaledb_catalog.compression_chunk_size ccs
            join _timescaledb_catalog.chunk c on (c.id = ccs.chunk_id)
            join pg_class cl on (cl.relname = c.table_name)
            where cl.oid in (chunk, new_chunk);

        if chunk_stats_before.uncompressed_heap_size = uncompressed_heap_size_sum and
           chunk_stats_before.compressed_heap_size = compressed_heap_size_sum
        then
           raise notice 'compression size stats are OK';
        else
           raise exception 'compression size stats are different after split: '
                 'uncompressed size % vs % and compressed size % vs %',
                 chunk_stats_before.uncompressed_heap_size,
                 uncompressed_heap_size_sum,
                 chunk_stats_before.compressed_heap_size,
                 compressed_heap_size_sum;
        end if;
     end if;
end;
$$ language plpgsql;

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
select create_hypertable('splitme', 'time', chunk_time_interval => interval '1 week', create_default_indexes => false);
alter table splitme set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='device');

-- Create information view for test
create view chunk_info as
select relname as chunk, amname as tam, reltuples, con.conname, pg_get_expr(conbin, ch) checkconstraint
from pg_class cl
join pg_am am on (cl.relam = am.oid)
join show_chunks('splitme') ch on (cl.oid = ch)
join pg_constraint con on (con.conrelid = ch)
where con.contype = 'c'
order by 1,2,3 desc;

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

select * from chunk_info;
call split_chunk_validate('_timescaledb_internal._hyper_1_1_chunk', split_at => '2024-01-04 00:00');
select * from chunk_info;

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
from generate_series('2024-01-03 23:00'::timestamptz, '2024-01-09 01:00:00 PST', '10s') t;
select count(*) from splitme;

-- Add back location just to make things more difficult
alter table splitme add column location int default 1;

-- Update stats
vacuum analyze splitme;

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

-- Add an index
create index on splitme (time);

-- Generate a garbage tuple
insert into _timescaledb_internal._hyper_1_2_chunk (time, device, location, temp) values ('2024-01-04 23:00', 1, 1, 1.12);
delete from _timescaledb_internal._hyper_1_2_chunk where time = '2024-01-04 23:00' and device = 1 and location = 1;

-- This will split in two equal size chunks
select * from chunk_info;
call split_chunk_validate('_timescaledb_internal._hyper_1_2_chunk');
select * from chunk_info;

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

-- Remove comment column to generate dropped column
alter table splitme drop column comment;

select * from chunk_info;
\c :TEST_DBNAME :ROLE_SUPERUSER
set role :ROLE_DEFAULT_PERM_USER;
select setseed(0.2);

select * from chunk_slices where hypertable_name = 'splitme';

insert into splitme (time, device, location, temp)
select t, ceil(random()*10), ceil(random()*20), random()*40
from generate_series('2024-01-03'::timestamptz, '2024-01-09 01:00:00 PST', '10s') t;

-- Make sure stats are up-to-date before split
vacuum analyze splitme;

select * from chunk_info;
call split_chunk_validate('_timescaledb_internal._hyper_1_2_chunk');
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
       ('2024-01-09 14:00', 1, 2, 2.0);

select ch as chunk_md from show_chunks('splitme_md') ch limit 1 \gset
select * from chunk_slices where hypertable_name = 'splitme_md';
\set ON_ERROR_STOP 0
-- Currently can't split multi-dimensional chunks due to bug/limitation in subspace store.
call split_chunk_validate(:'chunk_md');
\set ON_ERROR_STOP 1

-- Split when insert in progress
begin;
insert into splitme values ('2024-01-04 22:00', 20, 20, 20.0);
call split_chunk_validate('_timescaledb_internal._hyper_1_1_chunk');
rollback;

-- Split when delete in progress
begin;
delete from splitme where device = 1;
call split_chunk_validate('_timescaledb_internal._hyper_1_1_chunk');
rollback;

-------------------------------------------
-------------------------------------------
--- Split a compressed/columnstore chunk
-------------------------------------------
-------------------------------------------

-- Convert one chunk to columnstore
call convert_to_columnstore('_timescaledb_internal._hyper_1_2_chunk');

-- Compute aggregations to compare with after split
create table chunk_summary_before_split as
select
    count(*),
    sum(device) as device_sum,
    sum(location) as location_sum,
    round(sum(temp)::numeric, 5) as temp_sum
    from _timescaledb_internal._hyper_1_2_chunk;

-- Split the columnstore chunk, fully compressed
select compress_relid from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_2_chunk'::regclass \gset

-- All data should be in the (internal) compressed chunk
select count(*) from only _timescaledb_internal._hyper_1_2_chunk;
select count(*) from :compress_relid;

-- Use split point Thu Jan 04 13:40:30 2024 PST which should be within
-- some segments
select 'Thu Jan 04 13:40:30 2024 PST'::timestamptz as split_point \gset
select _ts_meta_count, device, _ts_meta_min_1, _ts_meta_max_1 from :compress_relid
where _ts_meta_min_1 <= :'split_point' and _ts_meta_max_1 >= :'split_point';

call split_chunk_validate('_timescaledb_internal._hyper_1_2_chunk', split_at => :'split_point');
select * from chunk_info;
select * from chunk_slices where hypertable_name = 'splitme';
select show_chunks('splitme');

-- Still no data in non-compressed relations after split.
select * from only _timescaledb_internal._hyper_1_2_chunk;
select * from only _timescaledb_internal._hyper_1_13_chunk;

-- Show how compressed segments are split across the resulting
-- compressed chunks
select compress_relid from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_2_chunk'::regclass \gset
select _ts_meta_count, device, _ts_meta_min_1, _ts_meta_max_1
from :compress_relid;

select compress_relid from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_13_chunk'::regclass \gset
select _ts_meta_count, device, _ts_meta_min_1, _ts_meta_max_1
from :compress_relid;

-- Would normally do a UNION ALL between the chunks, but it is broken
-- on compressed chunks
create table chunk_data_after_split as
select * from _timescaledb_internal._hyper_1_2_chunk;
insert into chunk_data_after_split select * from _timescaledb_internal._hyper_1_13_chunk;

-- All data should be compressed so these queries should not return
-- anything
select * from only _timescaledb_internal._hyper_1_2_chunk;
select * from only _timescaledb_internal._hyper_1_13_chunk;

create table chunk_summary_after_split as
select
    count(*),
    sum(device) as device_sum,
    sum(location) as location_sum,
    round(sum(temp)::numeric, 5) as temp_sum
    from chunk_data_after_split;

select * from chunk_summary_before_split;
select * from chunk_summary_after_split;

-- Split a hypercore TAM chunk
alter table _timescaledb_internal._hyper_1_3_chunk set access method hypercore;

-- Add some non-compressed data. One tuple in each partition after
-- split.
insert into _timescaledb_internal._hyper_1_3_chunk values
('2024-01-07 09:03:11', 1, 21, 21.0),
('2024-01-10 02:04:12', 1, 17, 32.0);

select *
from _timescaledb_internal._hyper_1_3_chunk
where _timescaledb_debug.is_compressed_tid(ctid) = false
order by time;

select table_name, dropped, status, compressed_chunk_id
from _timescaledb_catalog.chunk where table_name = '_hyper_1_3_chunk';

truncate chunk_summary_before_split;
insert into chunk_summary_before_split
select
    count(*),
    sum(device) as device_sum,
    sum(location) as location_sum,
    round(sum(temp)::numeric, 5) as temp_sum
    from _timescaledb_internal._hyper_1_3_chunk;

select * from chunk_summary_before_split;
select * from chunk_info;
call split_chunk('_timescaledb_internal._hyper_1_3_chunk');

-- Check that the resulting chunks look OK and have the right access method
select * from chunk_info;

select table_name, dropped, status, compressed_chunk_id
from _timescaledb_catalog.chunk
where table_name in ('_hyper_1_3_chunk', '_hyper_1_16_chunk');

-- Check that the non-compressed data rows ended up in separate partitions
select *
from _timescaledb_internal._hyper_1_3_chunk
where _timescaledb_debug.is_compressed_tid(ctid) = false
order by time;

select *
from _timescaledb_internal._hyper_1_16_chunk
where _timescaledb_debug.is_compressed_tid(ctid) = false
order by time;

-- Show aggregate summary. Should be equal to summary before split
truncate chunk_data_after_split;
insert into chunk_data_after_split select * from _timescaledb_internal._hyper_1_3_chunk;
insert into chunk_data_after_split select * from _timescaledb_internal._hyper_1_16_chunk;

truncate chunk_summary_after_split;
insert into chunk_summary_after_split
select
    count(*),
    sum(device) as device_sum,
    sum(location) as location_sum,
    round(sum(temp)::numeric, 5) as temp_sum
    from chunk_data_after_split;

-- Compare summaries before and after split
select * from chunk_summary_before_split;
select * from chunk_summary_after_split;

-- Show the summary for each new chunk
select
    count(*),
    sum(device) as device_sum,
    sum(location) as location_sum,
    round(sum(temp)::numeric, 5) as temp_sum
    from _timescaledb_internal._hyper_1_3_chunk;

select
    count(*),
    sum(device) as device_sum,
    sum(location) as location_sum,
    round(sum(temp)::numeric, 5) as temp_sum
    from _timescaledb_internal._hyper_1_16_chunk;


select chunk_name, range_start, range_end, is_compressed
from timescaledb_information.chunks
where hypertable_name = 'splitme'
order by chunk_name, range_start, range_end;

select compress_relid from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_16_chunk'::regclass \gset
select count(*) from :compress_relid;

--------------------------------------------------------------------
--------------------------------------------------------------------
-- Split compressed chunk in a way that leaves no compressed data in
-- one of the chunks
--------------------------------------------------------------------
--------------------------------------------------------------------
call convert_to_columnstore('_timescaledb_internal._hyper_1_8_chunk');
select max(time) as split_point from _timescaledb_internal._hyper_1_8_chunk \gset
select :'split_point';

call split_chunk_validate('_timescaledb_internal._hyper_1_8_chunk', split_at => :'split_point'::timestamptz + interval '1 second');

--
-- Show new chunk ranges. Note that, because the original chunk was
-- compressed, both result chunks are also "compressed" even though
-- one of them has no compressed data.
--
-- The chunk without compressed data could have been marked
-- "uncompressed" and the compressed relation removed. But this is not
-- how things work currently.
--
select chunk_name, range_start, range_end, is_compressed
from timescaledb_information.chunks
where hypertable_name = 'splitme'
order by chunk_name, range_start, range_end;

select compress_relid from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_8_chunk'::regclass \gset
select count(*) from :compress_relid;

select compress_relid from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_19_chunk'::regclass \gset
select count(*) from :compress_relid;

select count(*) from _timescaledb_internal._hyper_1_8_chunk;
select count(*) from _timescaledb_internal._hyper_1_19_chunk;

--------------------------------------------------------------------
--------------------------------------------------------------------
--- Test split points at time min and max values of compressed
--- segment. We'd like to check that these values end up in the
--- "right" result chunk after split. The split point should end up in
--- the "right" result chunk, i.e., it should be a min value in a new
--- segment.
--------------------------------------------------------------------
--------------------------------------------------------------------
select compress_relid from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_13_chunk'::regclass \gset

select sum(_ts_meta_count) as original_count_sum from :compress_relid \gset
select _ts_meta_count as meta_count, _ts_meta_min_1 as meta_min, _ts_meta_max_1 as meta_max, _ts_meta_min_1 as split_point_min, _ts_meta_max_1 as split_point_max
from :compress_relid order by meta_min limit 1 offset 2 \gset

-- Show metadata from the segment to be split. There might be other
-- segments split too, but we are only intrested in this one segment
-- for the test.
select :'meta_count' as meta_count, :'split_point_min' as split_point_min, :'split_point_max' as split_point_max;

-- Split with a split point at min value
call split_chunk_validate('_timescaledb_internal._hyper_1_13_chunk', split_at => :'split_point_min');


select compress_relid as compress_relid2 from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_21_chunk'::regclass \gset

-- The segment should remain intact, and be completely in the "new"
-- chunk (to the "right side" of the split)
select _ts_meta_count, _ts_meta_min_1 as meta_min, _ts_meta_max_1 as meta_max
from :compress_relid
where _ts_meta_min_1 = :'split_point_min' and _ts_meta_count = :'meta_count'
order by meta_min;

select _ts_meta_count, _ts_meta_min_1 as meta_min, _ts_meta_max_1 as meta_max
from :compress_relid2
where _ts_meta_min_1 = :'split_point_min' and _ts_meta_count = :'meta_count'
order by meta_min;


-- The sum of the counts should match up with the original sum before
-- split
select :'original_count_sum' = ((select sum(_ts_meta_count) from :compress_relid) + (select sum(_ts_meta_count) from :compress_relid2));

-- Use a split point at end of a segment (its max value). The segment
-- should be split with the max value ending up as the only value in a
-- new segment to the "right side" of the split (i.e., in the new
-- chunk)
select sum(_ts_meta_count) as original_count_sum from :compress_relid2 \gset
call split_chunk_validate('_timescaledb_internal._hyper_1_21_chunk', split_at => :'split_point_max');

select compress_relid as compress_relid3 from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_23_chunk'::regclass \gset

select _ts_meta_count, _ts_meta_min_1 as meta_min, _ts_meta_max_1 as meta_max
from :compress_relid2
where _ts_meta_min_1 = :'split_point_min' and _ts_meta_count = (:'meta_count' - 1)
order by meta_min;

select _ts_meta_count, _ts_meta_min_1 as meta_min, _ts_meta_max_1 as meta_max
from :compress_relid3
where _ts_meta_min_1 = :'split_point_max' and _ts_meta_count = 1
order by meta_min;

-- The sums of the counts should match up
select :'original_count_sum' = ((select sum(_ts_meta_count) from :compress_relid2) + (select sum(_ts_meta_count) from :compress_relid3));

select * from chunk_info;
select count(*), min(time), max(time) from _timescaledb_internal._hyper_1_23_chunk;
select max(time) as max_time from _timescaledb_internal._hyper_1_23_chunk \gset


--------------------------------------------------------
--------------------------------------------------------
-- Split a partial chunk so that the compressed data ends up in one
-- chunk and the non-compressed data in the other.
--------------------------------------------------------
--------------------------------------------------------

-- Create a new chunk
insert into splitme (time, device, temp)
select t, ceil(random()*10), random()*40
from generate_series('2024-01-10 23:00'::timestamptz, '2024-01-12 22:00:00 PST', '10s') t;

call convert_to_columnstore('_timescaledb_internal._hyper_1_25_chunk');

select * from chunk_info where chunk = '_hyper_1_25_chunk';

-- Check that all data is compressed
select compress_relid as compress_relid
from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_25_chunk'::regclass \gset

select count(*) from only _timescaledb_internal._hyper_1_25_chunk;
select count(*) from :compress_relid;

-- Insert non-compressed data
insert into splitme (time, device, temp)
select t, ceil(random()*10), random()*40
from generate_series('2024-01-12 23:00'::timestamptz, '2024-01-16 22:00:00 PST', '10s') t;

select count(*) from only _timescaledb_internal._hyper_1_25_chunk;
select count(*) from :compress_relid;

-- Split at a point between the compressed and non-compressed data
call split_chunk_validate('_timescaledb_internal._hyper_1_25_chunk', split_at => '2024-01-12 22:30'::timestamptz);

select * from chunk_info where chunk in ('_hyper_1_25_chunk', '_hyper_1_27_chunk');

-- Check that the distribution of data is such that one chunk has only
-- compressed data and the other only non-compressed data.
select compress_relid as compress_relid2
from _timescaledb_catalog.compression_settings
where relid = '_timescaledb_internal._hyper_1_27_chunk'::regclass \gset

select count(*) from only _timescaledb_internal._hyper_1_25_chunk;
select count(*) from :compress_relid;
select count(*) from only _timescaledb_internal._hyper_1_27_chunk;
select count(*) from :compress_relid2;
