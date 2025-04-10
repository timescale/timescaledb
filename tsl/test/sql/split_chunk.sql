-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE ACCESS METHOD testam TYPE TABLE HANDLER heap_tableam_handler;
set role :ROLE_DEFAULT_PERM_USER;

create table splitme (time timestamptz not null, device int, location int, temp float);
select create_hypertable('splitme', 'time', 'device', 2, chunk_time_interval => interval '1 week');

--
-- Insert data to create two chunks with time ranges like this:
-- _____________
-- |     |     |
-- |  1  |  2  |
-- |_____|_____|
---
insert into splitme values
       ('2024-01-03 22:00', 1, 1, 1.0),
       ('2024-01-09 15:00', 1, 2, 2.0);

-- Remove a column to ensure that split can handle it
alter table splitme drop column location;

-- All data in single chunk
select chunk_name, range_start, range_end from timescaledb_information.chunks;
select * from _timescaledb_internal._hyper_1_1_chunk order by time;
select * from _timescaledb_catalog.dimension_slice;

\set ON_ERROR_STOP 0
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', 'foo');
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', 'device');
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', 'time', split_at => 1);
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', 'time', split_at => 1::int);
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', 'time', split_at => '2024-01-04 00:00'::timestamp);
-- split at multiple points. Not supported yet.
call split_chunk('_timescaledb_internal._hyper_1_1_chunk', 'time', split_at => '{ 2024-01-04 10:00, 2024-01-07 12:00 }'::timestamptz[]);

-- Split a chunk with unsupported access method
alter table _timescaledb_internal._hyper_1_1_chunk set access method testam;
call split_chunk('_timescaledb_internal._hyper_1_1_chunk');
alter table _timescaledb_internal._hyper_1_1_chunk set access method heap;

-- Split an OSM chunk
reset role;
update _timescaledb_catalog.chunk ch set osm_chunk = true where table_name = '_hyper_1_1_chunk';
set role :ROLE_DEFAULT_PERM_USER;

call split_chunk('_timescaledb_internal._hyper_1_1_chunk');
reset role;
update _timescaledb_catalog.chunk ch set osm_chunk = false where table_name = '_hyper_1_1_chunk';
set role :ROLE_DEFAULT_PERM_USER;

-- Split a frozen chunk
select _timescaledb_functions.freeze_chunk('_timescaledb_internal._hyper_1_1_chunk');
call split_chunk('_timescaledb_internal._hyper_1_1_chunk');
select _timescaledb_functions.unfreeze_chunk('_timescaledb_internal._hyper_1_1_chunk');
\set ON_ERROR_STOP 1

call split_chunk('_timescaledb_internal._hyper_1_1_chunk', split_at => '2024-01-04 00:00');

select chunk_name, range_start, range_end from timescaledb_information.chunks;
select * from _timescaledb_catalog.dimension_slice;
select * from show_chunks('splitme');

-- Show that the two tuples ended up in different chunks
select * from _timescaledb_internal._hyper_1_1_chunk order by time;
select * from _timescaledb_internal._hyper_1_2_chunk order by time;

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
select chunk_name, range_start, range_end from timescaledb_information.chunks;

-- Split chunk 2. Save count to compare after split.
select count(*) from _timescaledb_internal._hyper_1_2_chunk;
select count(*) orig_count from _timescaledb_internal._hyper_1_2_chunk \gset

-- Generate some garbage so that we can see that it gets cleaned up
-- during split
update  _timescaledb_internal._hyper_1_2_chunk set temp = temp+1 where temp > 10;

-- This will split in two equal size chunks
call split_chunk('_timescaledb_internal._hyper_1_2_chunk');

select chunk_name, range_start, range_end from timescaledb_information.chunks;

-- Check that the counts in the two result partitions is the same as
-- in the original partition and that the tuple counts are roughly the
-- same across the partitions.
with counts as (
    select (select count(*) from _timescaledb_internal._hyper_1_2_chunk) count1,
            (select count(*) from _timescaledb_internal._hyper_1_5_chunk) count2
) select
  c.count1, c.count2,
  c.count1 + c.count2 as total_count,
  (c.count1 + c.count2) = :orig_count as is_same_count
from counts c;

-- Check that both rels return proper data and no columns are messed
-- up
select * from _timescaledb_internal._hyper_1_2_chunk order by time, device limit 3;
select * from _timescaledb_internal._hyper_1_5_chunk order by time, device limit 3;
