-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- this test checks the validity of the produced plans for partially compressed chunks
-- when injecting query_pathkeys on top of the append
-- path that combines the uncompressed and compressed parts of a chunk.

set work_mem to '64MB';
set enable_hashagg to off;

\set PREFIX 'EXPLAIN (analyze, costs off, timing off, summary off)'

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

reset work_mem;
reset enable_hashagg;
