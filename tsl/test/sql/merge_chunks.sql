-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.


\c :TEST_DBNAME :ROLE_SUPERUSER
set role :ROLE_DEFAULT_PERM_USER;

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


select "Constraint", "Columns", "Expr" from test.show_constraints('_timescaledb_internal._hyper_1_1_chunk');

-- Show partition layout
select * from partitions;

-- Now merge chunk 1 and 2:
begin;
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_2_chunk');
select * from _timescaledb_internal._hyper_1_1_chunk;
select reltuples from pg_class where oid='_timescaledb_internal._hyper_1_1_chunk'::regclass;
select * from partitions;
select "Constraint", "Columns", "Expr" from test.show_constraints('_timescaledb_internal._hyper_1_1_chunk');
select count(*) as num_orphaned_slices from orphaned_slices;
select * from show_chunks('mergeme');
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
select "Constraint", "Columns", "Expr" from test.show_constraints('_timescaledb_internal._hyper_1_1_chunk');
select count(*) as num_orphaned_slices from orphaned_slices;
select * from show_chunks('mergeme');
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

\set ON_ERROR_STOP 0
-- cannot merge a compressed chunk
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_2_chunk');
call merge_chunks('_timescaledb_internal._hyper_1_2_chunk', '_timescaledb_internal._hyper_1_3_chunk');
\set ON_ERROR_STOP 1

select decompress_chunk('_timescaledb_internal._hyper_1_3_chunk');

-- Merging with Hypercore TAM works
select compress_chunk('_timescaledb_internal._hyper_1_1_chunk', hypercore_use_access_method=>true);
select relname, amname from pg_class cl
join pg_am am on (cl.relam = am.oid)
where cl.oid = '_timescaledb_internal._hyper_1_1_chunk'::regclass;

select * from _timescaledb_internal._hyper_1_1_chunk;
select * from _timescaledb_internal._hyper_1_2_chunk;

begin;
select * from mergeme;
call merge_chunks(ARRAY['_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_1_3_chunk', '_timescaledb_internal._hyper_1_2_chunk']);
select * from partitions;
-- Note that no space partition CHECK constraint is added because it
-- now covers the entire range from -inf to +inf.
select "Constraint", "Columns", "Expr" from test.show_constraints('_timescaledb_internal._hyper_1_1_chunk');
select count(*) as num_orphaned_slices from orphaned_slices;
select * from show_chunks('mergeme');

select relname, amname from pg_class cl
join pg_am am on (cl.relam = am.oid)
where cl.oid = '_timescaledb_internal._hyper_1_1_chunk'::regclass;
select _timescaledb_debug.is_compressed_tid(ctid), * from _timescaledb_internal._hyper_1_1_chunk;
select * from mergeme;
rollback;

---
-- Test some error cases when merging chunks with non-chunks or chunks
-- from other hypertables
---
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
call merge_chunks('_timescaledb_internal._hyper_1_1_chunk', '_timescaledb_internal._hyper_3_8_chunk');
\set ON_ERROR_STOP 1
