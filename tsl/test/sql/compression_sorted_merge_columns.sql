-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test various corner cases of sorting.

create table t(x int, time timestamp, timetz timestamptz, int32 int4, int64 int8, s text);
select create_hypertable('t', 'x');

insert into t values
(1, '2022-01-01', '2023-01-01',  5, -13, 'a'),
(2, '2023-01-01', '2025-01-01',  1, -13, 'b'),
(3, '2024-01-01', '2021-01-01',  3, -16, 'd'),
(4, '2025-01-01', '2021-01-01', -2,  12, 'e'),
(5, '2021-01-01', '2026-01-01', -2,  14, 'e'),
(6, '2021-01-01', '2022-01-01', -4,  15, 'c')
;

alter table t set (timescaledb.compress, timescaledb.compress_segmentby='x', timescaledb.compress_orderby='time');
select compress_chunk(show_chunks('t')) \gset

set enable_sort to off;
set timescaledb.enable_decompression_sorted_merge to on;
set timescaledb.debug_require_batch_sorted_merge to 'require';
-- set enable_sort to on;
-- set timescaledb.enable_decompression_sorted_merge to off;
-- set timescaledb.debug_require_batch_sorted_merge to off;

select * from t order by time;

-- Test the fix for #7975: batch sort merge over eligible OpExpr and FuncExpr
select time::timestamptz + interval '1 second' from t order by time::timestamptz + interval '1 second';
select time + interval '1 second' from t order by time + interval '1 second';

select decompress_chunk(show_chunks('t')) \gset
alter table t set (timescaledb.compress, timescaledb.compress_segmentby='x', timescaledb.compress_orderby='timetz,time');
select compress_chunk(show_chunks('t')) \gset

select * from t order by timetz, time;

select timetz::timestamp + interval '1 second' from t order by timetz::timestamp + interval '1 second';
select timetz + interval '1 second' from t order by timetz + interval '1 second';

select decompress_chunk(show_chunks('t')) \gset
alter table t set (timescaledb.compress, timescaledb.compress_segmentby='x', timescaledb.compress_orderby='int32,timetz,time');
select compress_chunk(show_chunks('t')) \gset

select * from t order by int32, timetz, time;

select int32 + 1 from t order by int32 + 1;
-- Sort Merge not used because only last Order By expression can be sort-transformed, not both of them
\set ON_ERROR_STOP 0
select int32 + 1, timetz  + interval '1 second' from t order by int32 + 1, timetz  + interval '1 second';
\set ON_ERROR_STOP 1

select decompress_chunk(show_chunks('t')) \gset
alter table t set (timescaledb.compress, timescaledb.compress_segmentby='x', timescaledb.compress_orderby='int64,int32,timetz,time');
select compress_chunk(show_chunks('t')) \gset

select * from t order by int64, int32, timetz, time;

-- Both sides of OpExpr have to be of the same type for sort_transform to work
select int64 + 1 from t order by int64 + 1::int8;
-- Batch sort merge won't be used as sides of OpExpr are of different types (int8 vs int4)
\set ON_ERROR_STOP 0
select int64 + 1 from t order by int64 + 1;
\set ON_ERROR_STOP 1

select decompress_chunk(show_chunks('t')) \gset
alter table t set (timescaledb.compress, timescaledb.compress_segmentby='x', timescaledb.compress_orderby='s,int32,time desc');
select compress_chunk(show_chunks('t')) \gset

select * from t order by s, int32, time desc;

-- Batch sort merge won't be used as only Int and Date/Time types are allowed in OpExpr
\set ON_ERROR_STOP 0
select s||'1' from t order by s||'1';
\set ON_ERROR_STOP 1
