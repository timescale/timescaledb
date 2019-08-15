-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.


-- this should use DecompressChunk node
:PREFIX SELECT * FROM metrics WHERE device_id = 1 ORDER BY time LIMIT 5;

-- test RECORD by itself
:PREFIX SELECT * FROM metrics WHERE device_id = 1 ORDER BY time;

-- test expressions
:PREFIX SELECT
  time_bucket('1d',time),
  v1 + v2 AS "sum",
  COALESCE(NULL,v1,v2) AS "coalesce",
  NULL AS "NULL",
  'text' AS "text",
  metrics AS "RECORD"
FROM metrics WHERE device_id IN (1,2) ORDER BY time, device_id;

-- test empty targetlist
:PREFIX SELECT FROM metrics;

-- test empty resultset
:PREFIX SELECT * FROM metrics WHERE device_id < 0;

-- test targetlist not referencing columns
:PREFIX SELECT 1 FROM metrics;

-- test constraints not present in targetlist
:PREFIX SELECT v1 FROM metrics WHERE device_id = 1 ORDER BY v1;

-- test order not present in targetlist
:PREFIX SELECT v2 FROM metrics WHERE device_id = 1 ORDER BY v1;

-- test column with all NULL
:PREFIX SELECT v3 FROM metrics WHERE device_id = 1;

--
-- test qual pushdown
--

-- time is not segment by column so should not be pushed down
:PREFIX SELECT * FROM metrics WHERE time < '2000-01-08' ORDER BY time, device_id;

-- device_id constraint should be pushed down
:PREFIX SELECT * FROM metrics WHERE device_id = 1 ORDER BY time, device_id LIMIT 10;

-- test ANY constraint on segmentby column
:PREFIX SELECT * FROM metrics WHERE device_id IN (1,2) ORDER BY time, device_id LIMIT 10;

-- test cast pushdown
:PREFIX SELECT * FROM metrics WHERE device_id = '1'::text::int ORDER BY time, device_id LIMIT 10;

-- test expressions
:PREFIX SELECT * FROM metrics WHERE device_id =  1 + 4/2 ORDER BY time, device_id LIMIT 10;

-- test function calls
:PREFIX SELECT * FROM metrics WHERE device_id = length(substring(version() from 1 for 3)) ORDER BY time, device_id LIMIT 10;

--
-- test constraint exclusion
--

-- test plan time exclusion
-- first chunk should be excluded
:PREFIX SELECT * FROM metrics WHERE time > '2000-01-08' ORDER BY time, device_id;

-- test runtime exclusion
-- first chunk should be excluded
:PREFIX SELECT * FROM metrics WHERE time > '2000-01-08'::text::timestamptz ORDER BY time, device_id;

-- test aggregate
:PREFIX SELECT count(*) FROM metrics;

-- test aggregate with GROUP BY
:PREFIX SELECT count(*) FROM metrics GROUP BY device_id ORDER BY device_id;

-- test window functions with GROUP BY
:PREFIX SELECT sum(count(*)) OVER () FROM metrics GROUP BY device_id ORDER BY device_id;

-- test CTE
:PREFIX WITH
q AS (SELECT v1 FROM metrics ORDER BY time)
SELECT * FROM q ORDER BY v1;

-- test CTE join
:PREFIX WITH
q1 AS (SELECT time, v1 FROM metrics WHERE device_id=1 ORDER BY time),
q2 AS (SELECT time, v2 FROM metrics WHERE device_id=2 ORDER BY time)
SELECT * FROM q1 INNER JOIN q2 ON q1.time=q2.time ORDER BY q1.time;

-- test prepared statement
PREPARE prep AS SELECT count(time) FROM metrics WHERE device_id = 1;
:PREFIX EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
DEALLOCATE prep;

-- test explicit self-join
-- XXX FIXME
-- :PREFIX SELECT * FROM metrics m1 INNER JOIN metrics m2 ON m1.time = m2.time ORDER BY m1.time;

-- test implicit self-join
-- XXX FIXME
-- :PREFIX SELECT * FROM metrics m1, metrics m2 WHERE m1.time = m2.time ORDER BY m1.time;

-- test self-join with sub-query
-- XXX FIXME
-- :PREFIX SELECT * FROM (SELECT * FROM metrics m1) m1 INNER JOIN (SELECT * FROM metrics m2) m2 ON m1.time = m2.time ORDER BY m1.time;

-- test system columns
-- XXX FIXME
--SELECT xmin FROM metrics ORDER BY time;

