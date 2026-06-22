-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- this test checks the validity of the produced plans for partially compressed chunks
-- when injecting query_pathkeys on top of the append
-- path that combines the uncompressed and compressed parts of a chunk.

set work_mem to '64MB';
set enable_hashagg to off;

\set PREFIX 'EXPLAIN (analyze, buffers off, costs off, timing off, summary off)'

CREATE TABLE ht_metrics_partially_compressed(time timestamptz, device int, value float);
SELECT create_hypertable('ht_metrics_partially_compressed','time');
ALTER TABLE ht_metrics_partially_compressed SET (timescaledb.compress,
    timescaledb.compress_segmentby='device', timescaledb.compress_orderby='time');

INSERT INTO ht_metrics_partially_compressed
SELECT time, device, device * 0.1
FROM generate_series('2020-01-02'::timestamptz,'2020-01-18'::timestamptz,'20 minute') time,
generate_series(1,3) device;

SELECT compress_chunk(c) FROM show_chunks('ht_metrics_partially_compressed') c;
-- make them partially compressed
INSERT INTO ht_metrics_partially_compressed
SELECT time, device, device * 0.1
FROM generate_series('2020-01-02'::timestamptz,'2020-01-18'::timestamptz,'30 minute') time,
generate_series(1,3) device;

VACUUM ANALYZE ht_metrics_partially_compressed;

-- sort transform

-- Grouping can use compressed data order.
:PREFIX
select device, time_bucket('1 minute', time), count(*)
from ht_metrics_partially_compressed
group by 1, 2 order by 1, 2 limit 1;

-- Should be disabled when disabling optimizations.
SET timescaledb.enable_optimizations TO false;

:PREFIX
select device, time_bucket('1 minute', time), count(*)
from ht_metrics_partially_compressed
group by 1, 2 order by 1, 2 limit 1;

RESET timescaledb.enable_optimizations;

-- Batch sorted merge.
:PREFIX
select time_bucket('1 minute', time), count(*)
from ht_metrics_partially_compressed
group by 1 order by 1 limit 1;

-- Batch sorted merge with different order in SELECT list.
:PREFIX
select count(*), time_bucket('1 minute', time)
from ht_metrics_partially_compressed
group by 2 order by 2 limit 1;

-- Batch sorted merge with grouping column not in SELECT list.
:PREFIX
select count(*)
from ht_metrics_partially_compressed
group by time_bucket('1 minute', time) limit 1;

-- Ordering by time_bucket.
:PREFIX
select time_bucket('1 minute', time), *
from ht_metrics_partially_compressed
order by 1 limit 1;

-- Ordering by time_bucket, but it's not in the SELECT list.
:PREFIX
select * from ht_metrics_partially_compressed
order by time_bucket('1 minute', time) limit 1;

-- Ordering in compressed data order.
:PREFIX
select * from ht_metrics_partially_compressed
order by device, time_bucket('1 minute', time) limit 1;

-- Test incorrect transformation into a Pathkey on different relation through
-- a join EquivalenceClass.
set max_parallel_workers_per_gather = 0;
:PREFIX
select time_bucket('1 minute', a.time) from ht_metrics_partially_compressed a
join ht_metrics_partially_compressed b
on a.time = b.time
where b.time < '2020-01-07'
group by 1
;
reset max_parallel_workers_per_gather;

reset work_mem;
reset enable_hashagg;

-- Sorting on a set-returning function.
set max_parallel_workers_per_gather to 0;

create table srf_sort(time timestamptz not null, dev int, seg int);
select create_hypertable('srf_sort', 'time', chunk_time_interval => interval '1 day');
insert into srf_sort values
    ('2025-01-01', 3, 1),
    ('2025-01-02', 2, 1),
    ('2025-01-03', 4, 2);
alter table srf_sort set (timescaledb.compress, timescaledb.compress_segmentby='seg', timescaledb.compress_orderby='time');
select count(compress_chunk(c)) from show_chunks('srf_sort') c;

-- Sorting by the set-returning function with a LIMIT.
select generate_series(1, dev) g from srf_sort order by g limit 5;
explain (costs off) select generate_series(1, dev) g from srf_sort order by g limit 5;

-- Without a LIMIT it produces the same data.
select generate_series(1, dev) g from srf_sort order by g;

-- A set-returning function only in the output, sorting on a real column.
explain (costs off) select generate_series(1, dev) g, time from srf_sort order by time limit 3;

-- Sorting on a real column followed by a set-returning function.
select dev, generate_series(1, dev) g from srf_sort order by dev, g limit 4;

-- Sorting on a segment column followed by a set-returning function.
explain (costs off) select seg, generate_series(1, dev) g from srf_sort order by seg, g limit 4;
select seg, generate_series(1, dev) g from srf_sort order by seg, g limit 4;

drop table srf_sort cascade;
reset max_parallel_workers_per_gather;
