-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\echo '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'
\echo '%%% RUNNING TESTS on table:' :TABLE_NAME
\echo '%%% PREFIX:' :PREFIX
\echo '%%% WHERE_CLAUSE:' :WHERE_CLAUSE
\echo '%%% ORDER_BY_1:' :ORDER_BY_1
\echo '%%% ORDER_BY_1_2:' :ORDER_BY_1_2
\echo '%%% LIMIT:' :LIMIT
\echo '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%'
SELECT setseed(1);

-----------------------------------------------------------------
-- GROUP on time (partial aggregation)
-----------------------------------------------------------------
\set TEST_DESC '\n######### Grouping on time only (partial aggregation)\n'
\qecho :TEST_DESC
:PREFIX
SELECT time, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1
:ORDER_BY_1
:LIMIT
:OUTPUT_CMD


\qecho :TEST_DESC
:PREFIX
SELECT time_bucket('2 days', time) AS time, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1
:ORDER_BY_1
:LIMIT
:OUTPUT_CMD

-----------------------------------------------------------------
-- GROUP on time/time_bucket,device (full aggregation)
-----------------------------------------------------------------
\set TEST_DESC '\n######### Grouping on time and device (full aggregation)\n'
\qecho :TEST_DESC
:PREFIX
SELECT time, device, avg(temp)
FROM hyper
WHERE :WHERE_CLAUSE
GROUP BY 1,2
:ORDER_BY_1_2
:LIMIT
:OUTPUT_CMD

\qecho :TEST_DESC
:PREFIX
SELECT time_bucket('2 days', time) AS time, device, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1,2
:ORDER_BY_1_2
:LIMIT
:OUTPUT_CMD

\qecho :TEST_DESC
:PREFIX
SELECT date_trunc('month', time) AS time, device, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1,2
:ORDER_BY_1_2
:LIMIT
:OUTPUT_CMD

\qecho :TEST_DESC
-- HAVING clause with no aggregates. Should be reduced to a simple
-- filter on the remote node.
:PREFIX
SELECT time_bucket('2 days', time) AS time, device, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1,2
HAVING device > 4
:ORDER_BY_1_2
:LIMIT
:OUTPUT_CMD

-- HAVING clause with aggregates. In case of partial aggregation, the
-- max(temp) agg should be pulled up into the target list for
-- partialization. The avg(temp) is already there, so should not be
-- pulled up again.
:PREFIX
SELECT time_bucket('2 days', time) AS time, device, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1,2
HAVING avg(temp) > 40 AND max(temp) < 70
:ORDER_BY_1_2
:LIMIT
:OUTPUT_CMD

-----------------------------------------------------------------
-- GROUP on device (full aggregation)
-----------------------------------------------------------------
\set TEST_DESC '\n######### Grouping on device only (full aggregation)\n'
\qecho :TEST_DESC
:PREFIX
SELECT device, avg(temp)
FROM hyper
WHERE :WHERE_CLAUSE
GROUP BY 1
:ORDER_BY_1
:LIMIT
:OUTPUT_CMD

-----------------------------------------------------------------
-- No push downs or some expressions not pushed down.  Note that the
-- qual with random() effectively filters no tuples in order to make
-- this test deterministic in the output between settings.
-----------------------------------------------------------------
\set TEST_DESC '\n######### No push down on some functions\n'
\qecho :TEST_DESC
:PREFIX
SELECT location, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE AND (temp * random() >= 0)
GROUP BY 1
:ORDER_BY_1
:LIMIT
:OUTPUT_CMD

\qecho :TEST_DESC
:PREFIX
SELECT time_bucket('2 days', time) AS time, device, avg(temp), sum(temp * (random() <= 1)::int) as sum
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1,2
:ORDER_BY_1_2
:LIMIT
:OUTPUT_CMD

\qecho :TEST_DESC
:PREFIX
SELECT time, device, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1,2
HAVING avg(temp) * custom_sum(device) > 0.8
:ORDER_BY_1_2
:LIMIT
:OUTPUT_CMD

\qecho :TEST_DESC
:PREFIX
SELECT time, device, avg(temp), custom_sum(device)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1,2
:ORDER_BY_1_2
:LIMIT
:OUTPUT_CMD

-----------------------------------------------------------------
-- Test constification and runtime push down of time-related
-- functions.
-----------------------------------------------------------------
\set TEST_DESC '\n######### Constification and runtime push down of time-related functions\n'
\qecho :TEST_DESC
SELECT test.tsl_override_current_timestamptz('2018-06-01 00:00'::timestamptz);

:PREFIX
SELECT time, device, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1, 2
:ORDER_BY_1_2
:LIMIT;

-- Verify that repeated runs of the same plan will get different timestamps
SELECT format('%s_stmt', :'TABLE_NAME')
AS prepared_stmt
\gset

PREPARE :prepared_stmt AS
SELECT time, device, avg(temp)
FROM :TABLE_NAME
WHERE :WHERE_CLAUSE
GROUP BY 1, 2
:ORDER_BY_1_2
:LIMIT;

:PREFIX
EXECUTE :prepared_stmt;
SELECT test.tsl_override_current_timestamptz('2019-10-15 00:00'::timestamptz);

:PREFIX
EXECUTE :prepared_stmt;

DEALLOCATE :prepared_stmt;

-----------------------------------------------------------------
-- LIMIT push down support
-----------------------------------------------------------------
\set TEST_DESC '\n######### LIMIT push down cases\n'

-- Basic query (should be pushed)
\qecho :TEST_DESC
:PREFIX
SELECT time, device
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 10
:OUTPUT_CMD

-- LIMIT with OFFSET
\qecho :TEST_DESC
:PREFIX
SELECT time, device
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 5
OFFSET 5
:OUTPUT_CMD

-- LIMIT 0 corner case (will be translated to 1)
\qecho :TEST_DESC
:PREFIX
SELECT time, device
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 0
:OUTPUT_CMD

-- LIMIT expr that could be constified safely
\qecho :TEST_DESC
:PREFIX
SELECT time, device
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT extract(year from date '2000-01-01')
:OUTPUT_CMD

-- No push downs for some LIMIT corner cases

-- LIMIT volatile expression
\qecho :TEST_DESC
:PREFIX
SELECT time, device
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT greatest(random(), 10.0)
:OUTPUT_CMD

-- Window function
\qecho :TEST_DESC
:PREFIX
SELECT time, device, avg(temp) OVER (PARTITION BY device)
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 10
:OUTPUT_CMD

-- Ensure that using DISTINCT and DISTINCT ON prevent LIMIT push down
\qecho :TEST_DESC
:PREFIX
SELECT DISTINCT device, time
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 10
:OUTPUT_CMD

\qecho :TEST_DESC
:PREFIX
SELECT DISTINCT ON (device) device, time
FROM :TABLE_NAME
:ORDER_BY_1_2
LIMIT 10
:OUTPUT_CMD

-- JOIN with a local table
CREATE TABLE join_test (device int);

\qecho :TEST_DESC
:PREFIX
SELECT t.time
FROM :TABLE_NAME t, join_test
WHERE t.device = join_test.device
LIMIT 10;

DROP TABLE join_test;

-----------------------------------------------------------------
-- Test CTE / sub-queries. Data from two sub-queries on the same data
-- node is joined on the access node.
-----------------------------------------------------------------
\set TEST_DESC '\n######### CTEs/Sub-queries\n'

-- CTE / subquery
\qecho :TEST_DESC
:PREFIX
WITH top_n AS (
	 SELECT device, avg(temp)
	 FROM :TABLE_NAME
	 WHERE :WHERE_CLAUSE
     GROUP BY 1
	 ORDER BY 2 DESC
	 LIMIT 10
)
SELECT time_bucket('60s', time) AS "time", device, avg(temp)
FROM :TABLE_NAME INNER JOIN top_n USING (device)
WHERE :WHERE_CLAUSE
GROUP BY 1,2
ORDER BY 1,2
:OUTPUT_CMD

-- Join between two distributed hypertables
\qecho :TEST_DESC
:PREFIX
SELECT time_bucket('60s', h1.time) AS "time", h1.device, avg(h1.temp), max(h2.temp)
FROM hyper h1 INNER JOIN hyper1d h2 ON (time_bucket('60', h1.time) = time_bucket('60', h2.time) AND h1.device = h2.device)
WHERE h1.time BETWEEN '2019-01-01' AND '2019-01-01 15:00' AND
	  h2.time BETWEEN '2019-01-01' AND '2019-01-01 15:00'
GROUP BY 1,2
ORDER BY 1,2
:OUTPUT_CMD

