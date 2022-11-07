-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--parallel queries require big-ish tables so collect them all here
--so that we need to generate queries only once.

-- output with analyze is not stable because it depends on worker assignment
\set PREFIX 'EXPLAIN (costs off)'

\set CHUNK1 _timescaledb_internal._hyper_1_1_chunk
\set CHUNK2 _timescaledb_internal._hyper_1_2_chunk

CREATE TABLE test (i int, j double precision, ts timestamp);
SELECT create_hypertable('test','i',chunk_time_interval:=500000);
INSERT INTO test SELECT x, x+0.1, _timescaledb_internal.to_timestamp(x*1000)  FROM generate_series(0,1000000-1,10) AS x;

ANALYZE test;
ALTER TABLE :CHUNK1 SET (parallel_workers=2);
ALTER TABLE :CHUNK2 SET (parallel_workers=2);

SET work_mem TO '50MB';
SET force_parallel_mode = 'on';
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost TO 0;

EXPLAIN (costs off) SELECT first(i, j) FROM "test";
SELECT first(i, j) FROM "test";

EXPLAIN (costs off) SELECT last(i, j) FROM "test";
SELECT last(i, j) FROM "test";

EXPLAIN (costs off) SELECT time_bucket('1 second', ts) sec, last(i, j)
FROM "test"
GROUP BY sec
ORDER BY sec
LIMIT 5;

-- test single copy parallel plan with parallel chunk append
:PREFIX SELECT time_bucket('1 second', ts) sec, last(i, j)
FROM "test"
WHERE length(version()) > 0
GROUP BY sec
ORDER BY sec
LIMIT 5;

SELECT time_bucket('1 second', ts) sec, last(i, j)
FROM "test"
GROUP BY sec
ORDER BY sec
LIMIT 5;

--test variants of histogram
EXPLAIN (costs off) SELECT histogram(i, 1, 1000000, 2) FROM "test";
SELECT histogram(i, 1, 1000000, 2) FROM "test";

EXPLAIN (costs off) SELECT histogram(i, 1,1000001,10) FROM "test";
SELECT histogram(i, 1, 1000001, 10) FROM "test";

EXPLAIN (costs off) SELECT histogram(i, 0,100000,5) FROM "test";
SELECT histogram(i, 0, 100000, 5) FROM "test";

EXPLAIN (costs off) SELECT histogram(i, 10,100000,5) FROM "test";
SELECT histogram(i, 10, 100000, 5) FROM "test";

EXPLAIN (costs off) SELECT histogram(NULL, 10,100000,5) FROM "test" WHERE  i = coalesce(-1,j);
SELECT histogram(NULL, 10,100000,5) FROM "test" WHERE  i = coalesce(-1,j);

-- test parallel ChunkAppend
:PREFIX SELECT i FROM "test" WHERE length(version()) > 0;

-- test worker assignment
-- first chunk should have 1 worker and second chunk should have 2
SET max_parallel_workers_per_gather TO 2;
:PREFIX SELECT count(*) FROM "test" WHERE i >= 400000 AND length(version()) > 0;
SELECT count(*) FROM "test" WHERE i >= 400000 AND length(version()) > 0;

-- test worker assignment
-- first chunk should have 2 worker and second chunk should have 1
:PREFIX SELECT count(*) FROM "test" WHERE i < 600000 AND length(version()) > 0;
SELECT count(*) FROM "test" WHERE i < 600000 AND length(version()) > 0;

-- test ChunkAppend with # workers < # childs
SET max_parallel_workers_per_gather TO 1;
:PREFIX SELECT count(*) FROM "test" WHERE length(version()) > 0;
SELECT count(*) FROM "test" WHERE length(version()) > 0;

-- test ChunkAppend with # workers > # childs
SET max_parallel_workers_per_gather TO 2;
:PREFIX SELECT count(*) FROM "test" WHERE i >= 500000 AND length(version()) > 0;
SELECT count(*) FROM "test" WHERE i >= 500000 AND length(version()) > 0;

RESET max_parallel_workers_per_gather;

-- test partial and non-partial plans
-- these will not be parallel on PG < 11
ALTER TABLE :CHUNK1 SET (parallel_workers=0);
ALTER TABLE :CHUNK2 SET (parallel_workers=2);
:PREFIX SELECT count(*) FROM "test" WHERE i > 400000 AND length(version()) > 0;

ALTER TABLE :CHUNK1 SET (parallel_workers=2);
ALTER TABLE :CHUNK2 SET (parallel_workers=0);
:PREFIX SELECT count(*) FROM "test" WHERE i < 600000 AND length(version()) > 0;

ALTER TABLE :CHUNK1 RESET (parallel_workers);
ALTER TABLE :CHUNK2 RESET (parallel_workers);

-- now() is not marked parallel safe in PostgreSQL < 12 so using now()
-- in a query will prevent parallelism but CURRENT_TIMESTAMP and
-- transaction_timestamp() are marked parallel safe
:PREFIX SELECT i FROM "test" WHERE ts < CURRENT_TIMESTAMP;

:PREFIX SELECT i FROM "test" WHERE ts < transaction_timestamp();

-- this won't be parallel query because now() is parallel restricted in PG < 12
:PREFIX SELECT i FROM "test" WHERE ts < now();

