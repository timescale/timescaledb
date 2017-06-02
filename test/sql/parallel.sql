--parallel queries require big-ish tables so collect them all here 
--so that we need to generate queries only once.

\ir include/create_single_db.sql

CREATE TABLE test (i int, j double precision, ts timestamp);
--has to be big enough to force at least 2 workers below.
INSERT INTO test SELECT x, x+0.1, _timescaledb_internal.to_timestamp(x*1000)  FROM generate_series(1,1000000) AS x;

SET force_parallel_mode = 'on';
SET max_parallel_workers_per_gather = 4;

EXPLAIN (costs off) SELECT first(i, j) FROM "test";
SELECT first(i, j) FROM "test";
EXPLAIN (costs off) SELECT last(i, j) FROM "test";
SELECT last(i, j) FROM "test";

EXPLAIN (costs off) SELECT time_bucket('1 second', ts) sec, last(i, j) FROM "test" GROUP BY sec ORDER BY sec LIMIT 5;
SELECT time_bucket('1 second', ts) sec, last(i, j) FROM "test" GROUP BY sec ORDER BY sec LIMIT 5;

