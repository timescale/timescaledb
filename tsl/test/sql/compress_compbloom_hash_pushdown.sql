-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- all tests here will frst run with hash pushdown disabled and then
-- enabled for comparison

create table hash_pushdown_test(a int, b int, c int, d int)
with (
    tsdb.hypertable,
    tsdb.compress,
    tsdb.partition_column = 'c',
    tsdb.segmentby = '',
    tsdb.orderby = 'd',
    tsdb.compress_index = 'bloom(a), bloom(a,b)'
);

insert into hash_pushdown_test select x, x, x, x from generate_series(1, 10000) x;
select count(compress_chunk(x)) from show_chunks('hash_pushdown_test') x;
vacuum full analyze hash_pushdown_test;

-----------------------------------
-- hash pushdown disabled
-----------------------------------
set timescaledb.enable_bloom1_hash_pushdown = false;

-- single bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = 1;

-- composite bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = 1 and b = 2;

-- saop with single bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = any(array[1,2,3]);

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a IN (1,2,3);

-- saop with composite bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = any(array[1,2,3]) and b = any(array[4,5,6]);

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a IN (1,2,3) and b IN (4,5,6);

-----------------------------------
-- hash pushdown enabled
-----------------------------------
set timescaledb.enable_bloom1_hash_pushdown = true;

-- single bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = 1;

-- composite bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = 1 and b = 2;

-- saop with single bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = any(array[1,2,3]);

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a IN (1,2,3);

-- saop with composite bloom
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a = any(array[1,2,3]) and b = any(array[4,5,6]);

EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from hash_pushdown_test where a IN (1,2,3) and b IN (4,5,6);

-----------------------------------
-- cleanup
-----------------------------------
reset timescaledb.enable_bloom1_hash_pushdown;
drop table if exists hash_pushdown_test;
