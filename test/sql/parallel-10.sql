--parallel queries require big-ish tables so collect them all here
--so that we need to generate queries only once.

CREATE TABLE test (i int, j double precision, ts timestamp);
--has to be big enough to force at least 2 workers below.
INSERT INTO test SELECT x, x+0.1, _timescaledb_internal.to_timestamp(x*1000)  FROM generate_series(1,1000000) AS x;

SET client_min_messages = 'error';
--avoid warning polluting output
ANALYZE;
RESET client_min_messages;

SET force_parallel_mode = 'on';
SET max_parallel_workers_per_gather = 4;

EXPLAIN (costs off)
SELECT first(i, j) FROM "test";
SELECT first(i, j) FROM "test";

EXPLAIN (costs off)
SELECT last(i, j) FROM "test";
SELECT last(i, j) FROM "test";

EXPLAIN (costs off)
SELECT time_bucket('1 second', ts) sec, last(i, j)
FROM "test"
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
