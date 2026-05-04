-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table dml_test(
    ts int,
    o bigint,
    boo bigint,
    foo bigint,
    sby bigint);
select create_hypertable('dml_test', 'ts', create_default_indexes=>false);
insert into dml_test select x, (30000-x), x%2000, x+100, x%5 from generate_series(1, 50000) x;
alter table dml_test set (
    timescaledb.compress,
    timescaledb.order_by='o',
    timescaledb.segment_by='sby',
    timescaledb.compress_index = 'bloom(foo, boo), bloom(boo)');

select count(compress_chunk(x)) from show_chunks('dml_test') x;
vacuum analyze dml_test;

-- 2 filter clauses
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from dml_test where foo = 1001 and boo = 1002;

SET timescaledb.enable_dml_bloom_filter = off;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE dml_test SET foo = foo+1 WHERE foo = 1001 AND boo = 1002;
ROLLBACK;

SET timescaledb.enable_dml_bloom_filter = on;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE dml_test SET foo = foo+1 WHERE foo = 1001 AND boo = 1002;
ROLLBACK;

SET timescaledb.enable_dml_bloom_filter = off;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM dml_test WHERE foo = 1001 AND boo = 1002;
ROLLBACK;

SET timescaledb.enable_dml_bloom_filter = on;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM dml_test WHERE foo = 1001 AND boo = 1002;
ROLLBACK;


-- 1 filter clause
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from dml_test where foo = 12;

SET timescaledb.enable_dml_bloom_filter = off;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE dml_test SET foo = foo+1 WHERE foo = 12;
ROLLBACK;

SET timescaledb.enable_dml_bloom_filter = on;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE dml_test SET foo = foo+1 WHERE foo = 12;
ROLLBACK;

SET timescaledb.enable_dml_bloom_filter = off;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM dml_test WHERE foo = 12;
ROLLBACK;

SET timescaledb.enable_dml_bloom_filter = on;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM dml_test WHERE foo = 12;
ROLLBACK;

-- 1 filter clause, real updates
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
select * from dml_test where foo = 1002;

SET timescaledb.enable_dml_bloom_filter = off;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE dml_test SET foo = foo+1 WHERE foo = 1002;
ROLLBACK;

SET timescaledb.enable_dml_bloom_filter = on;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
UPDATE dml_test SET foo = foo+1 WHERE foo = 1002;
ROLLBACK;

SET timescaledb.enable_dml_bloom_filter = off;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM dml_test WHERE foo = 1002;
ROLLBACK;

SET timescaledb.enable_dml_bloom_filter = on;
BEGIN;
EXPLAIN (ANALYZE, BUFFERS OFF, COSTS OFF, TIMING OFF, SUMMARY OFF)
DELETE FROM dml_test WHERE foo = 1002;
ROLLBACK;
