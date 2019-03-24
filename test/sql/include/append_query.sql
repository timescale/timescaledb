-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- query should exclude all chunks with optimization on
:PREFIX
SELECT * FROM append_test WHERE time > now_s() + '1 month'
ORDER BY time DESC;

--query should exclude all chunks and be a MergeAppend
:PREFIX
SELECT * FROM append_test WHERE time > now_s() + '1 month'
ORDER BY time DESC limit 1;

-- when optimized, the plan should be a constraint-aware append and
-- cover only one chunk. It should be a backward index scan due to
-- descending index on time. Should also skip the main table, since it
-- cannot hold tuples
:PREFIX
SELECT * FROM append_test WHERE time > now_s() - interval '2 months';

-- adding ORDER BY and LIMIT should turn the plan into an optimized
-- ordered append plan
:PREFIX
SELECT * FROM append_test WHERE time > now_s() - interval '2 months'
ORDER BY time LIMIT 3;

-- no optimized plan for queries with restrictions that can be
-- constified at planning time. Regular planning-time constraint
-- exclusion should occur.
:PREFIX
SELECT * FROM append_test WHERE time > now_i() - interval '2 months'
ORDER BY time;

-- currently, we cannot distinguish between stable and volatile
-- functions as far as applying our modified plan. However, volatile
-- function should not be pre-evaluated to constants, so no chunk
-- exclusion should occur.
:PREFIX
SELECT * FROM append_test WHERE time > now_v() - interval '2 months'
ORDER BY time;

-- prepared statement output should be the same regardless of
-- optimizations
PREPARE query_opt AS
SELECT * FROM append_test WHERE time > now_s() - interval '2 months'
ORDER BY time;

:PREFIX EXECUTE query_opt;

DEALLOCATE query_opt;

-- aggregates should produce same output
:PREFIX
SELECT date_trunc('year', time) t, avg(temp) FROM append_test
WHERE time > now_s() - interval '4 months'
GROUP BY t
ORDER BY t DESC;

-- querying outside the time range should return nothing. This tests
-- that ConstraintAwareAppend can handle the case when an Append node
-- is turned into a Result node due to no children
:PREFIX
SELECT date_trunc('year', time) t, avg(temp)
FROM append_test
WHERE time < '2016-03-22'
AND date_part('dow', time) between 1 and 5
GROUP BY t
ORDER BY t DESC;

-- a parameterized query can safely constify params, so won't be
-- optimized by constraint-aware append since regular constraint
-- exclusion works just fine
PREPARE query_param AS
SELECT * FROM append_test WHERE time > $1 ORDER BY time;

:PREFIX
EXECUTE query_param(now_s() - interval '2 months');
DEALLOCATE query_param;

--test with cte
:PREFIX
WITH data AS (
    SELECT time_bucket(INTERVAL '30 day', TIME) AS btime, AVG(temp) AS VALUE
    FROM append_test
    WHERE
        TIME > now_s() - INTERVAL '400 day'
    AND colorid > 0
    GROUP BY btime
),
period AS (
    SELECT time_bucket(INTERVAL '30 day', TIME) AS btime
      FROM  GENERATE_SERIES('2017-03-22T01:01:01', '2017-08-23T01:01:01', INTERVAL '30 day') TIME
  )
SELECT period.btime, VALUE
    FROM period
    LEFT JOIN DATA USING (btime)
    ORDER BY period.btime;

WITH data AS (
    SELECT time_bucket(INTERVAL '30 day', TIME) AS btime, AVG(temp) AS VALUE
    FROM append_test
    WHERE
        TIME > now_s() - INTERVAL '400 day'
    AND colorid > 0
    GROUP BY btime
),
period AS (
    SELECT time_bucket(INTERVAL '30 day', TIME) AS btime
      FROM  GENERATE_SERIES('2017-03-22T01:01:01', '2017-08-23T01:01:01', INTERVAL '30 day') TIME
  )
SELECT period.btime, VALUE
    FROM period
    LEFT JOIN DATA USING (btime)
    ORDER BY period.btime;

-- force nested loop join with no materialization. This tests that the
-- inner ConstraintAwareScan supports resetting its scan for every
-- iteration of the outer relation loop
set enable_hashjoin = 'off';
set enable_mergejoin = 'off';
set enable_material = 'off';

:PREFIX
SELECT * FROM append_test a INNER JOIN join_test j ON (a.colorid = j.colorid)
WHERE a.time > now_s() - interval '3 hours' AND j.time > now_s() - interval '3 hours';

