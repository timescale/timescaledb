-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set EXPLAIN 'EXPLAIN (COSTS OFF)'

CREATE TABLE gapfill_plan_test(time timestamptz NOT NULL, value float);
SELECT table_name FROM create_hypertable('gapfill_plan_test','time',chunk_time_interval=>'4 weeks'::interval);

INSERT INTO gapfill_plan_test SELECT generate_series('2018-01-01'::timestamptz,'2018-04-01'::timestamptz,'1m'::interval), 1.0;

-- simple example
:EXPLAIN
SELECT
  time_bucket_gapfill('5m',time,now(),now()),
  avg(c2)
FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
GROUP BY 1
ORDER BY 1;

-- test sorting
:EXPLAIN
SELECT
  time_bucket_gapfill('5m',time,now(),now()),
  avg(c2)
FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
GROUP BY 1
ORDER BY 2;

-- test sort direction
:EXPLAIN
SELECT
  time_bucket_gapfill('5m',time,now(),now()),
  avg(c2)
FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
GROUP BY 1
ORDER BY 1 DESC;

-- test order by aggregate function
:EXPLAIN
SELECT
  time_bucket_gapfill('5m',time,now(),now()),
  avg(c2)
FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
GROUP BY 1
ORDER BY 2,1;

-- test query without order by
:EXPLAIN
SELECT
  time_bucket_gapfill('5m',time,now(),now()),
  avg(c2)
FROM (VALUES (now(),1),(now(),NULL),(now(),NULL)) as t(time,c2)
GROUP BY 1;

-- test parallel query
:EXPLAIN
SELECT
  time_bucket_gapfill('5m',time,to_timestamp(0),to_timestamp(0)),
  avg(value)
FROM gapfill_plan_test
GROUP BY 1
ORDER BY 1;

-- test parallel query with locf
:EXPLAIN
SELECT
  time_bucket_gapfill('5m',time,to_timestamp(0),to_timestamp(0)),
  locf(avg(value))
FROM gapfill_plan_test
GROUP BY 1
ORDER BY 1;

-- test parallel query with interpolate
:EXPLAIN
SELECT
  time_bucket_gapfill('5m',time,to_timestamp(0),to_timestamp(0)),
  interpolate(avg(value))
FROM gapfill_plan_test
GROUP BY 1
ORDER BY 1;

-- make sure we can run gapfill in parallel workers
-- ensure this plan runs in parallel
:EXPLAIN
SELECT
  time_bucket_gapfill('5m',time,to_timestamp(0),to_timestamp(0)),
  interpolate(avg(value))
FROM gapfill_plan_test
GROUP BY 1
ORDER BY 2
LIMIT 1;

-- actually run a parallel gapfill
SELECT
  time_bucket_gapfill('5m',time,to_timestamp(0),to_timestamp(0)),
  interpolate(avg(value))
FROM gapfill_plan_test
GROUP BY 1
ORDER BY 2
LIMIT 1;

-- test sort optimizations

-- test sort optimization with single member order by,
-- should use index scan (no GapFill node for this one since we're not gapfilling)
:EXPLAIN SELECT time_bucket_gapfill('5m',time),value
FROM gapfill_plan_test
ORDER BY 1;

SET max_parallel_workers_per_gather TO 0;

-- test sort optimizations with locf
:EXPLAIN SELECT time_bucket_gapfill('5m',time,to_timestamp(0),to_timestamp(0)), locf(avg(value))
FROM gapfill_plan_test
GROUP BY 1
ORDER BY 1;

-- test sort optimizations with interpolate
:EXPLAIN SELECT time_bucket_gapfill('5m',time,to_timestamp(0),to_timestamp(0)), interpolate(avg(value))
FROM gapfill_plan_test
GROUP BY 1
ORDER BY 1;

RESET max_parallel_workers_per_gather;

CREATE INDEX ON gapfill_plan_test(value, time);

-- test sort optimization with ordering by multiple columns and time_bucket_gapfill not last,
-- must not use index scan
:EXPLAIN  SELECT time_bucket_gapfill('5m',time),value
FROM gapfill_plan_test
ORDER BY 1,2;

-- test sort optimization with ordering by multiple columns and time_bucket as last member,
-- should use index scan
:EXPLAIN SELECT time_bucket_gapfill('5m',time),value
FROM gapfill_plan_test
ORDER BY 2,1;
