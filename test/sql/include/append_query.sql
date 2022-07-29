-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- canary for results diff
-- this should be the only output of the results diff
SELECT setting, current_setting(setting) AS value from (VALUES ('timescaledb.enable_optimizations'),('timescaledb.enable_chunk_append')) v(setting);

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

reset enable_hashjoin;
reset enable_mergejoin;
reset enable_material;

-- test constraint_exclusion with date time dimension and DATE/TIMESTAMP/TIMESTAMPTZ constraints
-- the queries should all have 3 chunks
:PREFIX SELECT * FROM metrics_date WHERE time > '2000-01-15'::date ORDER BY time;
:PREFIX SELECT * FROM metrics_date WHERE time > '2000-01-15'::timestamp ORDER BY time;
:PREFIX SELECT * FROM metrics_date WHERE time > '2000-01-15'::timestamptz ORDER BY time;

-- test Const OP Var
-- the queries should all have 3 chunks
:PREFIX SELECT * FROM metrics_date WHERE '2000-01-15'::date < time ORDER BY time;
:PREFIX SELECT * FROM metrics_date WHERE '2000-01-15'::timestamp < time ORDER BY time;
:PREFIX SELECT * FROM metrics_date WHERE '2000-01-15'::timestamptz < time ORDER BY time;

-- test 2 constraints
-- the queries should all have 2 chunks
:PREFIX SELECT * FROM metrics_date WHERE time > '2000-01-15'::date AND time < '2000-01-21'::date ORDER BY time;
:PREFIX SELECT * FROM metrics_date WHERE time > '2000-01-15'::timestamp AND time < '2000-01-21'::timestamp ORDER BY time;
:PREFIX SELECT * FROM metrics_date WHERE time > '2000-01-15'::timestamptz AND time < '2000-01-21'::timestamptz ORDER BY time;

-- test constraint_exclusion with timestamp time dimension and DATE/TIMESTAMP/TIMESTAMPTZ constraints
-- the queries should all have 3 chunks
:PREFIX SELECT * FROM metrics_timestamp WHERE time > '2000-01-15'::date ORDER BY time;
:PREFIX SELECT * FROM metrics_timestamp WHERE time > '2000-01-15'::timestamp ORDER BY time;
:PREFIX SELECT * FROM metrics_timestamp WHERE time > '2000-01-15'::timestamptz ORDER BY time;

-- test Const OP Var
-- the queries should all have 3 chunks
:PREFIX SELECT * FROM metrics_timestamp WHERE '2000-01-15'::date < time ORDER BY time;
:PREFIX SELECT * FROM metrics_timestamp WHERE '2000-01-15'::timestamp < time ORDER BY time;
:PREFIX SELECT * FROM metrics_timestamp WHERE '2000-01-15'::timestamptz < time ORDER BY time;

-- test 2 constraints
-- the queries should all have 2 chunks
:PREFIX SELECT * FROM metrics_timestamp WHERE time > '2000-01-15'::date AND time < '2000-01-21'::date ORDER BY time;
:PREFIX SELECT * FROM metrics_timestamp WHERE time > '2000-01-15'::timestamp AND time < '2000-01-21'::timestamp ORDER BY time;
:PREFIX SELECT * FROM metrics_timestamp WHERE time > '2000-01-15'::timestamptz AND time < '2000-01-21'::timestamptz ORDER BY time;

-- test constraint_exclusion with timestamptz time dimension and DATE/TIMESTAMP/TIMESTAMPTZ constraints
-- the queries should all have 3 chunks
:PREFIX SELECT time FROM metrics_timestamptz WHERE time > '2000-01-15'::date ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time > '2000-01-15'::timestamp ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time > '2000-01-15'::timestamptz ORDER BY time;

-- test Const OP Var
-- the queries should all have 3 chunks
:PREFIX SELECT time FROM metrics_timestamptz WHERE '2000-01-15'::date < time ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE '2000-01-15'::timestamp < time ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE '2000-01-15'::timestamptz < time ORDER BY time;

-- test 2 constraints
-- the queries should all have 2 chunks
:PREFIX SELECT time FROM metrics_timestamptz WHERE time > '2000-01-15'::date AND time < '2000-01-21'::date ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time > '2000-01-15'::timestamp AND time < '2000-01-21'::timestamp ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time > '2000-01-15'::timestamptz AND time < '2000-01-21'::timestamptz ORDER BY time;

-- test constraint_exclusion with space partitioning and DATE/TIMESTAMP/TIMESTAMPTZ constraints
-- exclusion for constraints with non-matching datatypes not working for space partitioning atm
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::date ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::timestamp ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::timestamptz ORDER BY time;

-- test Const OP Var
-- exclusion for constraints with non-matching datatypes not working for space partitioning atm
:PREFIX SELECT time FROM metrics_space WHERE '2000-01-10'::date < time ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE '2000-01-10'::timestamp < time ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE '2000-01-10'::timestamptz < time ORDER BY time;

-- test 2 constraints
-- exclusion for constraints with non-matching datatypes not working for space partitioning atm
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::date AND time < '2000-01-15'::date ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::timestamp AND time < '2000-01-15'::timestamp ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::timestamptz AND time < '2000-01-15'::timestamptz ORDER BY time;

-- test filtering on space partition
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::timestamptz AND device_id = 1 ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::timestamptz AND device_id IN (1,2) ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::timestamptz AND device_id IN (VALUES(1)) ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > '2000-01-10'::timestamptz AND v3 IN (VALUES('1')) ORDER BY time;

:PREFIX SELECT * FROM metrics_space
WHERE time = (VALUES ('2019-12-24' at time zone 'UTC'))
  AND v3 NOT IN (VALUES ('1'));

-- test CURRENT_DATE
-- should be 0 chunks
:PREFIX SELECT time FROM metrics_date WHERE time > CURRENT_DATE ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamp WHERE time > CURRENT_DATE ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time > CURRENT_DATE ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > CURRENT_DATE ORDER BY time;

-- test CURRENT_TIMESTAMP
-- should be 0 chunks
:PREFIX SELECT time FROM metrics_date WHERE time > CURRENT_TIMESTAMP ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamp WHERE time > CURRENT_TIMESTAMP ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time > CURRENT_TIMESTAMP ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > CURRENT_TIMESTAMP ORDER BY time;

-- test now()
-- should be 0 chunks
:PREFIX SELECT time FROM metrics_date WHERE time > now() ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamp WHERE time > now() ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time > now() ORDER BY time;
:PREFIX SELECT time FROM metrics_space WHERE time > now() ORDER BY time;

-- query with tablesample and planner exclusion
:PREFIX
SELECT * FROM metrics_date TABLESAMPLE BERNOULLI(5) REPEATABLE(0)
WHERE time > '2000-01-15'
ORDER BY time DESC;

-- query with tablesample and startup exclusion
:PREFIX
SELECT * FROM metrics_date TABLESAMPLE BERNOULLI(5) REPEATABLE(0)
WHERE time > '2000-01-15'::text::date
ORDER BY time DESC;

-- query with tablesample, space partitioning and planner exclusion
:PREFIX
SELECT * FROM metrics_space TABLESAMPLE BERNOULLI(5) REPEATABLE(0)
WHERE time > '2000-01-10'::timestamptz
ORDER BY time DESC, device_id;

-- test runtime exclusion

-- test runtime exclusion with LATERAL and 2 hypertables
:PREFIX SELECT m1.time, m2.time FROM metrics_timestamptz m1 LEFT JOIN LATERAL(SELECT time FROM metrics_timestamptz m2 WHERE m1.time = m2.time LIMIT 1) m2 ON true ORDER BY m1.time;

-- test runtime exclusion and startup exclusions
:PREFIX SELECT m1.time, m2.time FROM metrics_timestamptz m1 LEFT JOIN LATERAL(SELECT time FROM metrics_timestamptz m2 WHERE m1.time = m2.time AND m2.time < '2000-01-10'::text::timestamptz LIMIT 1) m2 ON true ORDER BY m1.time;

-- test runtime exclusion does not activate for constraints on non-partitioning columns
-- should not use runtime exclusion
:PREFIX SELECT * FROM append_test a LEFT JOIN LATERAL(SELECT * FROM join_test j WHERE a.colorid = j.colorid ORDER BY time DESC LIMIT 1) j ON true ORDER BY a.time LIMIT 1;

-- test runtime exclusion with LATERAL and generate_series
:PREFIX SELECT g.time FROM generate_series('2000-01-01'::timestamptz, '2000-02-01'::timestamptz, '1d'::interval) g(time) LEFT JOIN LATERAL(SELECT time FROM metrics_timestamptz m WHERE m.time=g.time LIMIT 1) m ON true;
:PREFIX SELECT * FROM generate_series('2000-01-01'::timestamptz,'2000-02-01'::timestamptz,'1d'::interval) AS g(time) INNER JOIN LATERAL (SELECT time FROM metrics_timestamptz m WHERE time=g.time) m ON true;
:PREFIX SELECT * FROM generate_series('2000-01-01'::timestamptz,'2000-02-01'::timestamptz,'1d'::interval) AS g(time) INNER JOIN LATERAL (SELECT time FROM metrics_timestamptz m WHERE time=g.time ORDER BY time) m ON true;
:PREFIX SELECT * FROM generate_series('2000-01-01'::timestamptz,'2000-02-01'::timestamptz,'1d'::interval) AS g(time) INNER JOIN LATERAL (SELECT time FROM metrics_timestamptz m WHERE time>g.time + '1 day' ORDER BY time LIMIT 1) m ON true;

-- test runtime exclusion with subquery
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 WHERE m1.time=(SELECT max(time) FROM metrics_timestamptz);

-- test runtime exclusion with correlated subquery
:PREFIX SELECT m1.time, (SELECT m2.time FROM metrics_timestamptz m2 WHERE m2.time < m1.time ORDER BY m2.time DESC LIMIT 1) FROM metrics_timestamptz m1 WHERE m1.time < '2000-01-10' ORDER BY m1.time;

-- test EXISTS
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 WHERE EXISTS(SELECT 1 FROM metrics_timestamptz m2 WHERE m1.time < m2.time) ORDER BY m1.time DESC limit 1000;

-- test constraint exclusion for subqueries with append
-- should include 2 chunks
:PREFIX SELECT time FROM (SELECT time FROM metrics_timestamptz WHERE time < '2000-01-10'::text::timestamptz ORDER BY time) m;

-- test constraint exclusion for subqueries with mergeappend
-- should include 2 chunks
:PREFIX SELECT device_id, time FROM (SELECT device_id, time FROM metrics_timestamptz WHERE time < '2000-01-10'::text::timestamptz ORDER BY device_id, time) m;

-- test LIMIT pushdown
-- no aggregates/window functions/SRF should pushdown limit
:PREFIX SELECT FROM metrics_timestamptz ORDER BY time LIMIT 1;

-- aggregates should prevent pushdown
:PREFIX SELECT count(*) FROM metrics_timestamptz LIMIT 1;
:PREFIX SELECT count(*) FROM metrics_space LIMIT 1;

-- HAVING should prevent pushdown
:PREFIX SELECT 1 FROM metrics_timestamptz HAVING count(*) > 1 LIMIT 1;
:PREFIX SELECT 1 FROM metrics_space HAVING count(*) > 1 LIMIT 1;

-- DISTINCT should prevent pushdown
SET enable_hashagg TO false;
:PREFIX SELECT DISTINCT device_id FROM metrics_timestamptz ORDER BY device_id LIMIT 3;
:PREFIX SELECT DISTINCT device_id FROM metrics_space ORDER BY device_id LIMIT 3;
RESET enable_hashagg;

-- JOINs should prevent pushdown
-- when LIMIT gets pushed to a Sort node it will switch to top-N heapsort
-- if more tuples then LIMIT are requested this will trigger an error
-- to trigger this we need a Sort node that is below ChunkAppend
CREATE TABLE join_limit (time timestamptz, device_id int);
SELECT table_name FROM create_hypertable('join_limit','time',create_default_indexes:=false);
CREATE INDEX ON join_limit(time,device_id);

INSERT INTO join_limit
SELECT time, device_id
FROM generate_series('2000-01-01'::timestamptz,'2000-01-21','30m') g1(time),
  generate_series(1,10,1) g2(device_id)
ORDER BY time, device_id;

-- get 2nd chunk oid
SELECT tableoid AS "CHUNK_OID" FROM join_limit WHERE time > '2000-01-07' ORDER BY time LIMIT 1
\gset
--get index name for 2nd chunk
SELECT indexrelid::regclass AS "INDEX_NAME" FROM pg_index WHERE indrelid = :CHUNK_OID
\gset
DROP INDEX :INDEX_NAME;

:PREFIX SELECT * FROM metrics_timestamptz m1 INNER JOIN join_limit m2 ON m1.time = m2.time AND m1.device_id=m2.device_id WHERE m1.time > '2000-01-07' ORDER BY m1.time, m1.device_id LIMIT 3;

DROP TABLE join_limit;

-- test ChunkAppend projection #2661
:PREFIX SELECT ts.timestamp, ht.timestamp
FROM (
  SELECT generate_series(
    to_timestamp(FLOOR(EXTRACT (EPOCH FROM '2020-01-01T00:01:00Z'::timestamp) / 300) * 300) AT TIME ZONE 'UTC',
    '2020-01-01T01:00:00Z',
    '5 minutes'::interval
  ) AS timestamp
) ts
LEFT JOIN i2661 ht ON
  (FLOOR(EXTRACT (EPOCH FROM ht."timestamp") / 300) * 300 = EXTRACT (EPOCH FROM ts.timestamp))
  AND ht.timestamp > '2019-12-30T00:00:00Z'::timestamp;

-- #3030 test chunkappend keeps pathkeys when subpath is append
-- on PG11 this will not use ChunkAppend but MergeAppend
SET enable_seqscan TO FALSE;
CREATE TABLE i3030(time timestamptz NOT NULL, a int, b int);
SELECT table_name FROM create_hypertable('i3030', 'time', create_default_indexes=>false);
CREATE INDEX ON i3030(a,time);
INSERT INTO i3030 (time,a) SELECT time, a FROM generate_series('2000-01-01'::timestamptz,'2000-01-01 3:00:00'::timestamptz,'1min'::interval) time, generate_series(1,30) a;
ANALYZE i3030;

:PREFIX SELECT * FROM i3030 where time BETWEEN '2000-01-01'::text::timestamptz AND '2000-01-03'::text::timestamptz ORDER BY a,time LIMIT 1;
DROP TABLE i3030;
RESET enable_seqscan;


--parent runtime exclusion tests:
--optimization works with ANY (array)
:PREFIX
SELECT *
FROM append_test a
WHERE a.attr @> ANY((SELECT coalesce(array_agg(attr), array[]::jsonb[]) FROM join_test_plain WHERE temp > 100)::jsonb[]);

--optimization does not work for ANY subquery (does not force an initplan)
:PREFIX
SELECT *
FROM append_test a
WHERE a.attr @> ANY((SELECT attr FROM join_test_plain WHERE temp > 100));

--works on any strict operator without ANY
:PREFIX
SELECT *
FROM append_test a
WHERE a.attr @> (SELECT attr FROM join_test_plain WHERE temp > 100 limit 1);


--optimization works with function calls
CREATE OR REPLACE FUNCTION select_tag(_min_temp int)
 RETURNS jsonb[]
 LANGUAGE sql
 STABLE PARALLEL SAFE
AS $function$
   SELECT coalesce(array_agg(attr), array[]::jsonb[])
  FROM join_test_plain
  WHERE temp > _min_temp
$function$;

:PREFIX
SELECT *
FROM append_test a
WHERE a.attr @> ANY((SELECT select_tag(100))::jsonb[]);

--optimization does not work when result is null
:PREFIX
SELECT *
FROM append_test a
WHERE a.attr @> ANY((SELECT array_agg(attr) FROM join_test_plain WHERE temp > 100)::jsonb[]);
