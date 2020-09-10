-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test ASC for ordered chunks
:PREFIX
SELECT time
FROM :TEST_TABLE
ORDER BY time ASC
LIMIT 1;

-- test DESC for ordered chunks
:PREFIX
SELECT time
FROM :TEST_TABLE
ORDER BY time DESC
LIMIT 1;

-- test query with ORDER BY column not in targetlist
:PREFIX
SELECT pg_typeof(device_id),
  pg_typeof(v2)
FROM :TEST_TABLE
ORDER BY time ASC
LIMIT 1;

-- ORDER BY may include other columns after time column
:PREFIX
SELECT time,
  device_id,
  v0
FROM :TEST_TABLE
WHERE device_id = 1
ORDER BY time DESC,
  device_id
LIMIT 1;

-- test RECORD in targetlist
:PREFIX
SELECT (time,
    device_id,
    v0)
FROM :TEST_TABLE
WHERE device_id = 1
ORDER BY time DESC,
  device_id
LIMIT 1;

-- test sort column not in targetlist
:PREFIX
SELECT time_bucket('1h', time)
FROM :TEST_TABLE
ORDER BY time DESC
LIMIT 1;

-- queries with ORDER BY non-time column shouldn't use ordered append
:PREFIX
SELECT device_id
FROM :TEST_TABLE
ORDER BY device_id
LIMIT 1;

-- time column must be primary sort order
:PREFIX
SELECT time,
  device_id
FROM :TEST_TABLE
WHERE device_id IN (1, 2)
ORDER BY device_id,
  time
LIMIT 1;

-- test equality constraint on ORDER BY prefix
-- currently not optimized
SET enable_seqscan TO false;
:PREFIX
SELECT time,
  device_id
FROM :TEST_TABLE
WHERE device_id = 1
ORDER BY device_id,
  time
LIMIT 10;
RESET enable_seqscan;

-- queries without LIMIT should use ordered append
:PREFIX
SELECT time
FROM :TEST_TABLE
WHERE device_id IN (1, 2)
ORDER BY time ASC;

-- queries without ORDER BY shouldnt use ordered append
:PREFIX
SELECT pg_typeof(time)
FROM :TEST_TABLE
LIMIT 1;

-- test interaction with constraint exclusion
:PREFIX
SELECT time
FROM :TEST_TABLE
WHERE time > '2000-01-07'
ORDER BY time ASC
LIMIT 1;

:PREFIX
SELECT time
FROM :TEST_TABLE
WHERE time > '2000-01-07'
ORDER BY time DESC
LIMIT 1;

-- test interaction with runtime exclusion
:PREFIX
SELECT time
FROM :TEST_TABLE
WHERE time > '2000-01-08'::text::timestamptz
ORDER BY time ASC
LIMIT 1;

:PREFIX
SELECT time
FROM :TEST_TABLE
WHERE time < '2000-01-08'::text::timestamptz
ORDER BY time ASC
LIMIT 1;

-- test constraint exclusion
:PREFIX
SELECT time
FROM :TEST_TABLE
WHERE time > '2000-01-08'::text::timestamptz
  AND time < '2000-01-10'
ORDER BY time ASC
LIMIT 1;

:PREFIX
SELECT time
FROM :TEST_TABLE
WHERE time < '2000-01-08'::text::timestamptz
  AND time > '2000-01-07'
ORDER BY time ASC
LIMIT 1;

-- min/max queries
:PREFIX
SELECT max(time)
FROM :TEST_TABLE;

:PREFIX
SELECT min(time)
FROM :TEST_TABLE;

-- test first/last (doesn't use ordered append yet)
:PREFIX
SELECT first(time, time)
FROM :TEST_TABLE;

:PREFIX
SELECT last(time, time)
FROM :TEST_TABLE;

-- test query with time_bucket
:PREFIX
SELECT time_bucket('1d', time)
FROM :TEST_TABLE
ORDER BY time ASC
LIMIT 1;

-- test query with ORDER BY time_bucket
:PREFIX
SELECT time_bucket('1d', time)
FROM :TEST_TABLE
ORDER BY 1
LIMIT 1;

-- test query with ORDER BY time_bucket, device_id
-- must not use ordered append
:PREFIX
SELECT time_bucket('1d', time),
  device_id,
  v0
FROM :TEST_TABLE
WHERE device_id IN (1, 2)
ORDER BY time_bucket('1d', time),
  device_id
LIMIT 1;

-- test query with ORDER BY date_trunc
:PREFIX
SELECT time_bucket('1d', time)
FROM :TEST_TABLE
ORDER BY date_trunc('day', time)
LIMIT 1;

-- test query with ORDER BY date_trunc
:PREFIX
SELECT date_trunc('day', time)
FROM :TEST_TABLE
ORDER BY 1
LIMIT 1;

-- test query with ORDER BY date_trunc, device_id
-- must not use ordered append
:PREFIX
SELECT date_trunc('day', time),
  device_id,
  v0
FROM :TEST_TABLE
WHERE device_id IN (1, 2)
ORDER BY 1,
  2
LIMIT 1;

-- test query with now() should result in ordered ChunkAppend
:PREFIX
SELECT time
FROM :TEST_TABLE
WHERE time < now() + '1 month'
ORDER BY time DESC
LIMIT 1;

-- test CTE
:PREFIX WITH i AS (
  SELECT time
  FROM :TEST_TABLE
  WHERE time < now()
  ORDER BY time DESC
  LIMIT 100
)
SELECT *
FROM i;

-- test CTE
-- no chunk exclusion for CTE because cte query is not pulled up
:PREFIX WITH cte AS (
  SELECT time
  FROM :TEST_TABLE
  WHERE device_id = 1
  ORDER BY time
)
SELECT *
FROM cte
WHERE time < '2000-02-01'::timestamptz;

-- test subquery
-- not ChunkAppend so no chunk exclusion
:PREFIX
SELECT time
FROM :TEST_TABLE
WHERE time = (
    SELECT max(time)
    FROM :TEST_TABLE)
ORDER BY time;

-- test ordered append with limit expression
:PREFIX
SELECT time
FROM :TEST_TABLE
ORDER BY time
LIMIT (
  SELECT length('four'));

-- test with ordered guc disabled
SET timescaledb.enable_ordered_append TO OFF;

:PREFIX
SELECT time
FROM :TEST_TABLE
ORDER BY time
LIMIT 3;

RESET timescaledb.enable_ordered_append;

:PREFIX
SELECT time
FROM :TEST_TABLE
ORDER BY time
LIMIT 3;

-- test with chunk append disabled
SET timescaledb.enable_chunk_append TO OFF;

:PREFIX
SELECT time
FROM :TEST_TABLE
ORDER BY time
LIMIT 3;

RESET timescaledb.enable_chunk_append;

:PREFIX
SELECT time
FROM :TEST_TABLE
ORDER BY time
LIMIT 3;
