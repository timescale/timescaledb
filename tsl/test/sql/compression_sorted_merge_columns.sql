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
set timescaledb.debug_require_batch_sorted_merge to on;
-- set enable_sort to on;
-- set timescaledb.enable_decompression_sorted_merge to off;
-- set timescaledb.debug_require_batch_sorted_merge to off;

select * from t order by time;


select decompress_chunk(show_chunks('t')) \gset
alter table t set (timescaledb.compress, timescaledb.compress_segmentby='x', timescaledb.compress_orderby='timetz,time');
select compress_chunk(show_chunks('t')) \gset

select * from t order by timetz, time;

select decompress_chunk(show_chunks('t')) \gset
alter table t set (timescaledb.compress, timescaledb.compress_segmentby='x', timescaledb.compress_orderby='int32,timetz,time');
select compress_chunk(show_chunks('t')) \gset

select * from t order by int32, timetz, time;

select decompress_chunk(show_chunks('t')) \gset
alter table t set (timescaledb.compress, timescaledb.compress_segmentby='x', timescaledb.compress_orderby='int64,int32,timetz,time');
select compress_chunk(show_chunks('t')) \gset

select * from t order by int64, int32, timetz, time;

select decompress_chunk(show_chunks('t')) \gset
alter table t set (timescaledb.compress, timescaledb.compress_segmentby='x', timescaledb.compress_orderby='s,int32,time desc');
select compress_chunk(show_chunks('t')) \gset

select * from t order by s, int32, time desc;
