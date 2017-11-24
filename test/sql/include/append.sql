-- create a now() function for repeatable testing that always returns
-- the same timestamp. It needs to be marked STABLE
CREATE OR REPLACE FUNCTION now_s()
RETURNS timestamptz LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RAISE NOTICE 'Stable function now_s() called!';
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE OR REPLACE FUNCTION now_i()
RETURNS timestamptz LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RAISE NOTICE 'Immutable function now_i() called!';
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE OR REPLACE FUNCTION now_v()
RETURNS timestamptz LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    RAISE NOTICE 'Volatile function now_v() called!';
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE TABLE append_test(time timestamptz, temp float, colorid integer);
SELECT create_hypertable('append_test', 'time', chunk_time_interval => 2628000000000);

-- create three chunks
INSERT INTO append_test VALUES ('2017-03-22T09:18:22', 23.5, 1),
                               ('2017-03-22T09:18:23', 21.5, 1),
                               ('2017-05-22T09:18:22', 36.2, 2),
                               ('2017-05-22T09:18:23', 15.2, 2),
                               ('2017-08-22T09:18:22', 34.1, 3);

-- query should exclude all chunks with optimization on
EXPLAIN (costs off)
SELECT * FROM append_test WHERE time > now_s() + '1 month';
SELECT * FROM append_test WHERE time > now_s() + '1 month';

--query should exclude all chunks and be a MergeAppend
EXPLAIN (costs off)
SELECT * FROM append_test WHERE time > now_s() + '1 month'
ORDER BY time DESC limit 1;

-- when optimized, the plan should be a constraint-aware append and
-- cover only one chunk. It should be a backward index scan due to
-- descending index on time. Should also skip the main table, since it
-- cannot hold tuples
EXPLAIN (costs off)
SELECT * FROM append_test WHERE time > now_s() - interval '2 months';

-- the expected output should be the same as the non-optimized query
SELECT * FROM append_test WHERE time > now_s() - interval '2 months';

-- adding ORDER BY and LIMIT should turn the plan into an optimized
-- merge append plan
EXPLAIN (costs off)
SELECT * FROM append_test WHERE time > now_s() - interval '2 months'
ORDER BY time LIMIT 3;

-- the expected output should be the same as the non-optimized query
SELECT * FROM append_test WHERE time > now_s() - interval '2 months'
ORDER BY time LIMIT 3;

-- no optimized plan for queries with restrictions that can be
-- constified at planning time. Regular planning-time constraint
-- exclusion should occur.
EXPLAIN (costs off)
SELECT * FROM append_test WHERE time > now_i() - interval '2 months'
ORDER BY time;

-- currently, we cannot distinguish between stable and volatile
-- functions as far as applying our modified plan. However, volatile
-- function should not be pre-evaluated to constants, so no chunk
-- exclusion should occur.
EXPLAIN (costs off)
SELECT * FROM append_test WHERE time > now_v() - interval '2 months'
ORDER BY time;

SELECT * FROM append_test WHERE time > now_v() - interval '2 months'
ORDER BY time;

-- prepared statement output should be the same regardless of
-- optimizations
PREPARE query_opt AS
SELECT * FROM append_test WHERE time > now_s() - interval '2 months'
ORDER BY time;

EXECUTE query_opt;

EXPLAIN (costs off)
EXECUTE query_opt;

-- aggregates should produce same output
SELECT date_trunc('year', time) t, avg(temp) FROM append_test
WHERE time > now_s() - interval '4 months'
GROUP BY t
ORDER BY t DESC;

-- aggregates should use optimized plan when possible
EXPLAIN (costs off)
SELECT date_trunc('year', time) t, avg(temp) FROM append_test
WHERE time > now_s() - interval '4 months'
GROUP BY t
ORDER BY t DESC;

-- querying outside the time range should return nothing. This tests
-- that ConstraintAwareAppend can handle the case when an Append node
-- is turned into a Result node due to no children
EXPLAIN (costs off)
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

EXPLAIN (costs off)
EXECUTE query_param(now_s() - interval '2 months');


-- Create another hypertable to join with
CREATE TABLE join_test(time timestamptz, temp float, colorid integer);
SELECT create_hypertable('join_test', 'time', chunk_time_interval => 2628000000000);

INSERT INTO join_test VALUES ('2017-01-22T09:18:22', 15.2, 1),
                             ('2017-02-22T09:18:22', 24.5, 2),
                             ('2017-08-22T09:18:22', 23.1, 3);

-- force nested loop join with no materialization. This tests that the
-- inner ConstraintAwareScan supports resetting its scan for every
-- iteration of the outer relation loop
set enable_hashjoin = 'off';
set enable_mergejoin = 'off';
set enable_material = 'off';

EXPLAIN (costs off)
SELECT * FROM append_test a INNER JOIN join_test j ON (a.colorid = j.colorid)
WHERE a.time > now_s() - interval '3 hours' AND j.time > now_s() - interval '3 hours';

-- result should be the same as when optimizations are turned off
SELECT * FROM append_test a INNER JOIN join_test j ON (a.colorid = j.colorid)
WHERE a.time > now_s() - interval '3 hours' AND j.time > now_s() - interval '3 hours';
