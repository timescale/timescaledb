-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.


\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE ACCESS METHOD testam TYPE TABLE HANDLER heap_tableam_handler;
set role :ROLE_DEFAULT_PERM_USER;
-- A limitation in the tuple routing cache can lead to routing errors
-- when multi-dimensional time partitions are not aligned. Therefore,
-- multi-dimensional merges are disabled by default until the routing
-- is fixed. However, allow it in this test.
set timescaledb.enable_merge_multidim_chunks = true;

------------------
-- Helper views --
-------------------
create view partitions as
select c.table_name, d.column_name, ds.range_start, ds.range_end
from _timescaledb_catalog.hypertable h
join _timescaledb_catalog.chunk c on (c.hypertable_id = h.id)
join _timescaledb_catalog.dimension d on (d.hypertable_id = h.id)
join _timescaledb_catalog.dimension_slice ds on (d.id = ds.dimension_id)
join _timescaledb_catalog.chunk_constraint cc on (cc.chunk_id = c.id and cc.dimension_slice_id = ds.id)
where h.table_name = 'mergeme'
order by d.id, ds.range_start, ds.range_end;

create view orphaned_slices as
select ds.id, cc.constraint_name from _timescaledb_catalog.dimension_slice ds
left join _timescaledb_catalog.chunk_constraint cc on (ds.id = cc.dimension_slice_id)
where cc.constraint_name is null;


-----------------
-- Setup table --
-----------------
create table mergeme (time timestamptz not null, device int, temp float);
select create_hypertable('mergeme', 'time', 'device', 3, chunk_time_interval => interval '1 day');

-- Create helper view for chunk information
create view chunk_info as
select relname as chunk, amname as tam, pg_get_expr(conbin, ch) checkconstraint
from pg_class cl
join pg_am am on (cl.relam = am.oid)
join show_chunks('mergeme') ch on (cl.oid = ch)
join pg_constraint con on (con.conrelid = ch)
where con.contype = 'c'
order by 1,2,3 desc;

--
-- Insert data to create two chunks with same time ranges like this:
-- _______
-- |     |
-- |  1  |
-- |_____|
-- |     |
-- |  2  |
-- |_____|
---
insert into mergeme values ('2024-01-01', 1, 1.0), ('2024-01-01', 2, 2.0);

-- Show chunks and check constraints
select * from chunk_info;

-- Show partition layout
select * from partitions;

-- Now merge chunk 1 and 2:
begin;
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_2_chunk');
select * from _timescaledb_internal._hyper_1_1_chunk;
select reltuples from pg_class where oid='_timescaledb_internal._hyper_1_1_chunk'::regclass;
select * from partitions;
select count(*) as num_orphaned_slices from orphaned_slices;
select * from chunk_info;
select * from mergeme;
rollback;


-- create a new chunk as a third space partition
-- _______
-- |     |
-- |  1  |
-- |_____|
-- |     |
-- |  2  |
-- |_____|
-- |     |
-- |  3  |
-- |_____|
---

insert into mergeme values ('2024-01-01', 3, 3.0);

-- Test some basic error cases
\set ON_ERROR_STOP 0
-- Can't merge chunk 1 and 3
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_3_chunk');
call merge_chunks(NULL);
call merge_chunks(NULL, NULL);
call merge_chunks(999999,999991);
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_1_chunk']);
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', NULL);
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_1_chunk', NULL]);
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_1_chunk');


-- Check permissions
reset role;
set role :ROLE_1;
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_2_chunk');
reset role;
set role :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 1

-- Show new partition
select * from partitions;

begin;
-- Should be able to merge all three chunks
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_3_chunk', '_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_2_chunk']);
select * from partitions;
-- Note that no space partition CHECK constraint is added because it
-- now covers the entire range from -inf to +inf.
select count(*) as num_orphaned_slices from orphaned_slices;
select * from chunk_info;
select * from mergeme;
rollback;

-- create two new chunks, 4 and 5, as follows:
-- _____________      _______
-- |     |     |      |     |
-- |  1  |  4  |      |  5  |
-- |_____|_____|      |_____|
-- |     |
-- |  2  |
-- |_____|
-- |     |
-- |  3  |
-- |_____|
---
insert into mergeme values ('2024-01-02', 1, 4.0), ('2024-01-04', 1, 5.0);

-- Show new partitions
select * from partitions;

\set ON_ERROR_STOP 0
-- can't merge 3 and 4
call merge_chunks('_timescaledb_internal._hyper_1_3_chunk', '_timescaledb_internal._hyper_1_4_chunk');
-- can't merge 1 and 5
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_5_chunk');
-- can't merge 2 and 4
call merge_chunks('_timescaledb_internal._hyper_1_2_chunk', '_timescaledb_internal._hyper_1_4_chunk');
-- can't merge 4 and 5
call merge_chunks('_timescaledb_internal._hyper_1_5_chunk', '_timescaledb_internal._hyper_1_4_chunk');
-- currently can't merge 1,2,3,4 due to limitation in how we validate the merge
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_3_chunk', '_timescaledb_internal._hyper_1_2_chunk', '_timescaledb_internal._hyper_1_4_chunk', '_timescaledb_internal._hyper_1_1_chunk']);

begin;
-- Should be able to merge all three chunks 1,2,3
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_2_chunk', '_timescaledb_internal._hyper_1_3_chunk']);
-- But merging the merged 1,2,3 chunk with 4 is currently not
-- possible, although we chould do it in theory
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_4_chunk');
rollback;
\set ON_ERROR_STOP 1

alter table mergeme set (timescaledb.compress_orderby='time', timescaledb.compress_segmentby='device');
select compress_chunk('_timescaledb_internal._hyper_1_1_chunk');
select compress_chunk('_timescaledb_internal._hyper_1_3_chunk');

-- Test merging compressed chunks
begin;
select * from chunk_info;

select * from _timescaledb_catalog.compression_chunk_size order by chunk_id;

call merge_chunks('{_timescaledb_internal._hyper_1_1_chunk, _timescaledb_internal._hyper_1_2_chunk, _timescaledb_internal._hyper_1_3_chunk}');
select * from _timescaledb_catalog.compression_chunk_size order by chunk_id;
select * from chunk_info;
select count(*) as num_orphaned_slices from orphaned_slices;
select * from mergeme;
rollback;

-- Test mixing hypercore TAM with compression without TAM
select * from chunk_info;

begin;
select sum(temp) from mergeme;
call merge_chunks('{_timescaledb_internal._hyper_1_1_chunk, _timescaledb_internal._hyper_1_2_chunk, _timescaledb_internal._hyper_1_3_chunk}');
select * from chunk_info;
select count(*) as num_orphaned_slices from orphaned_slices;
select sum(temp) from mergeme;
rollback;


select * from chunk_info;

-- Only Hypercore TAM and non-compressed chunks

begin;
select sum(temp) from mergeme;
call merge_chunks('{_timescaledb_internal._hyper_1_1_chunk, _timescaledb_internal._hyper_1_2_chunk, _timescaledb_internal._hyper_1_3_chunk}');
select * from chunk_info;
select count(*) as num_orphaned_slices from orphaned_slices;
select sum(temp) from mergeme;

-- Test that indexes work after merge
set timescaledb.enable_columnarscan = false;
set enable_seqscan = false;
analyze mergeme;
explain (buffers off, costs off)
select * from mergeme where device = 1;
select * from mergeme where device = 1;
select * from _timescaledb_internal._hyper_1_1_chunk where device = 1;
reset timescaledb.enable_columnarscan;
reset enable_seqscan;
rollback;

---
--- Merge hypercore TAM into compressed chunk without TAM
---
begin;
select * from chunk_info;
select compress_chunk('_timescaledb_internal._hyper_1_2_chunk');

select sum(temp) from mergeme;
call merge_chunks('{_timescaledb_internal._hyper_1_2_chunk, _timescaledb_internal._hyper_1_3_chunk}');
select * from chunk_info;
select count(*) as num_orphaned_slices from orphaned_slices;
select sum(temp) from mergeme;
rollback;

---
-- Test some error cases when merging chunks with non-chunks or chunks
-- from other hypertables
---
-- Decompress all chunks to ensure we only have non-compressed chunks
select decompress_chunk(ch) from show_chunks('mergeme') ch;

-- Create a non-chunk table
create table mergeme_too(time timestamptz not null, device int, temp float);
select create_hypertable('mergeme_too', 'time', 'device', 3, chunk_time_interval => interval '1 day');
create table mergeme_regular(time timestamptz not null, device int, temp float);

insert into mergeme_too values ('2024-01-01', 1, 1.0);
insert into mergeme_regular select * from mergeme_too;

create materialized view mergeme_mat as
select * from mergeme_too where device=1;

select * from show_chunks('mergeme_too');


\set ON_ERROR_STOP 0
-- Merge chunk and regular table
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', 'mergeme_regular');
call merge_chunks('mergeme_regular', '_timescaledb_internal._hyper_1_1_chunk');
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', 'mergeme_mat');
-- Merge chunks from different hypertables
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_3_9_chunk');

-- Merge with unsupported access method
alter table _timescaledb_internal._hyper_1_1_chunk set access method testam;
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_2_chunk');
alter table _timescaledb_internal._hyper_1_1_chunk set access method heap;

-- Merge OSM chunks
reset role;
update _timescaledb_catalog.chunk ch set osm_chunk = true where table_name = '_hyper_1_1_chunk';
set role :ROLE_DEFAULT_PERM_USER;

call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_2_chunk');
reset role;
update _timescaledb_catalog.chunk ch set osm_chunk = false where table_name = '_hyper_1_1_chunk';
set role :ROLE_DEFAULT_PERM_USER;

-- Merge frozen chunks
select _timescaledb_functions.freeze_chunk('_timescaledb_internal._hyper_1_1_chunk');
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_2_chunk');
call merge_chunks('_timescaledb_internal._hyper_1_2_chunk', '_timescaledb_internal._hyper_1_1_chunk');
select _timescaledb_functions.unfreeze_chunk('_timescaledb_internal._hyper_1_1_chunk');

\set ON_ERROR_STOP 1


-- Set seed to consistently generate same data and same set of chunks
select setseed(0.2);
-- Test merge with bigger data set and chunks with more blocks
insert into mergeme (time, device, temp)
select t, ceil(random()*10), random()*40
from generate_series('2024-01-01'::timestamptz, '2024-01-04', '0.5s') t;

-- Compress two chunks, one using access method
select compress_chunk('_timescaledb_internal._hyper_1_1_chunk');

-- Show partitions before merge
select * from partitions;

-- Show which chunks are compressed. Their compression_chunk_size
-- metadata should be merged.
select chunk_name from timescaledb_information.chunks
where is_compressed=true order by chunk_name;

--
-- Check that compression_chunk_size stats are also merged when we
-- merge compressed chunks.
--
-- Use a view to compare merged stats against the total sum of that
-- stats for all chunks. There are only two compressed chunks, 1 and
-- 2. Show each chunks stats as the fraction of the total size. This
-- is to make the test work across different architectures that show
-- slightly different absolute disk sizes.
---
select
    sum(ccs.uncompressed_heap_size) as total_uncompressed_heap_size,
    sum(ccs.uncompressed_toast_size) as total_uncompressed_toast_size,
    sum(ccs.uncompressed_index_size) as total_uncompressed_index_size,
    sum(ccs.compressed_heap_size) as total_compressed_heap_size,
    sum(ccs.compressed_toast_size) as total_compressed_toast_size,
    sum(ccs.compressed_index_size) as total_compressed_index_size,
    sum(ccs.numrows_pre_compression) as total_numrows_pre_compression,
    sum(ccs.numrows_post_compression) as total_numrows_post_compression,
    sum(ccs.numrows_frozen_immediately) as total_numrows_frozen_immediately
from _timescaledb_catalog.compression_chunk_size ccs \gset

-- View to show current chunk compression size stats as a fraction of
-- the totals.
create view compression_size_fraction as
select
    ccs.chunk_id,
    ccs.compressed_chunk_id,
    round(ccs.uncompressed_heap_size::numeric / :total_uncompressed_heap_size, 1) as uncompressed_heap_size_fraction,
    ccs.uncompressed_toast_size::numeric as uncompressed_toast_size_fraction,
    round(ccs.uncompressed_index_size::numeric / :total_uncompressed_index_size, 1) as uncompressed_index_size_fraction,
    round(ccs.compressed_heap_size::numeric / :total_compressed_heap_size, 1) as compressed_heap_size_fraction,
    round(ccs.compressed_toast_size::numeric / :total_compressed_toast_size, 1) as compressed_toast_size_fraction,
    round(ccs.compressed_index_size::numeric / :total_compressed_index_size, 1) as compressed_index_size_fraction,
    round(ccs.numrows_pre_compression ::numeric/ :total_numrows_pre_compression, 1) as numrows_pre_compression_fraction,
    round(ccs.numrows_post_compression::numeric / :total_numrows_post_compression, 1) as numrows_post_compression_fraction,
    round(ccs.numrows_frozen_immediately::numeric / :total_numrows_frozen_immediately, 1) as numrows_frozen_immediately_fraction
from _timescaledb_catalog.compression_chunk_size ccs
order by chunk_id;

\set ON_ERROR_STOP 0
-- Test blocked multi-dimensional merges
set timescaledb.enable_merge_multidim_chunks = false;
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_4_chunk','_timescaledb_internal._hyper_1_5_chunk', '_timescaledb_internal._hyper_1_12_chunk']);
set timescaledb.enable_merge_multidim_chunks = true;
\set ON_ERROR_STOP 1

--
-- Merge all chunks until only 1 remains.  Also check that metadata is
-- merged.
---
begin;
select * from compression_size_fraction;
select count(*), sum(device), round(sum(temp)::numeric, 4) from mergeme;
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_4_chunk','_timescaledb_internal._hyper_1_5_chunk', '_timescaledb_internal._hyper_1_12_chunk']);

select count(*), sum(device), round(sum(temp)::numeric, 4) from mergeme;
select * from partitions;
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_2_chunk', '_timescaledb_internal._hyper_1_10_chunk','_timescaledb_internal._hyper_1_13_chunk', '_timescaledb_internal._hyper_1_15_chunk']);

select count(*), sum(device), round(sum(temp)::numeric, 4) from mergeme;
select * from partitions;
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_3_chunk', '_timescaledb_internal._hyper_1_11_chunk','_timescaledb_internal._hyper_1_14_chunk', '_timescaledb_internal._hyper_1_16_chunk']);

-- Final merge, involving the two compressed chunks 1 and 2. The stats
-- should also be merged.
select * from compression_size_fraction;
select count(*), sum(device), round(sum(temp)::numeric, 4) from mergeme;
select * from partitions;
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_3_chunk', '_timescaledb_internal._hyper_1_1_chunk','_timescaledb_internal._hyper_1_2_chunk']);
select * from compression_size_fraction;
select count(*), sum(device), round(sum(temp)::numeric, 4) from mergeme;
select * from partitions;
select * from chunk_info;
rollback;

----------------------------
-- Test concurrent merges --
----------------------------

create view chunks_being_merged as
select
    chunk_relid,
    n as new_chunk_id,
    (select count(indexrelid::regclass) from pg_index where indrelid=new_relid) as num_new_indexes
from (select *, dense_rank() over (order by new_relid) as n from _timescaledb_catalog.chunk_rewrite) cr;

-- Concurrent merge cannot run in a transaction block
\set ON_ERROR_STOP 0
begin;
call merge_chunks_concurrently(ARRAY['_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_4_chunk','_timescaledb_internal._hyper_1_5_chunk', '_timescaledb_internal._hyper_1_12_chunk']);
rollback;

select debug_waitpoint_enable('merge_chunks_fail');
call merge_chunks_concurrently(ARRAY['_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_4_chunk','_timescaledb_internal._hyper_1_5_chunk', '_timescaledb_internal._hyper_1_12_chunk']);
\set ON_ERROR_STOP 1

select debug_waitpoint_release('merge_chunks_fail');
select * from chunks_being_merged;
create table pre_cleaned_chunks as select * from _timescaledb_catalog.chunk_rewrite;
call _timescaledb_functions.chunk_rewrite_cleanup();
select * from chunks_being_merged;

-- None of the pre-cleaned chunks should remain after cleanup. Check by joining
-- the pre-cleaned relids with pg_class. The count should be zero if the
-- relations no longer exist in pg_class.
select count(*) from pre_cleaned_chunks;
select count(*) from pre_cleaned_chunks cc join pg_class c on (c.oid = cc.new_relid);

drop table pre_cleaned_chunks;
drop view chunks_being_merged;
