-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- test prepared statements
-- executor startup exclusion with no chunks excluded

PREPARE prep AS
SELECT time
FROM :TEST_TABLE
WHERE time < now()
  AND device_id = 1
ORDER BY time
LIMIT 100;

:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- executor startup exclusion with chunks excluded
PREPARE prep AS
SELECT time
FROM :TEST_TABLE
WHERE time < '2000-01-10'::text::timestamptz
  AND device_id = 1
ORDER BY time
LIMIT 100;

:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- runtime exclusion with LATERAL and 2 hypertables
SET enable_seqscan TO false;

PREPARE prep AS
SELECT m1.time,
  m2.time
FROM :TEST_TABLE m1
  LEFT JOIN LATERAL (
    SELECT time
    FROM :TEST_TABLE m2
    WHERE m1.time = m2.time
    LIMIT 1) m2 ON TRUE
WHERE device_id = 2
ORDER BY m1.time
LIMIT 100;

:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

RESET enable_seqscan;

-- executor startup exclusion with subquery
PREPARE prep AS
SELECT time
FROM (
  SELECT time
  FROM :TEST_TABLE
  WHERE time < '2000-01-10'::text::timestamptz
  ORDER BY time
  LIMIT 100) m;

:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- test constraint exclusion for subqueries with ConstraintAwareAppend
SET timescaledb.enable_chunk_append TO FALSE;

PREPARE prep AS
SELECT device_id,
  time
FROM (
  SELECT device_id,
    time
  FROM :TEST_TABLE
  WHERE time < '2000-01-10'::text::timestamptz
  ORDER BY device_id,
    time
  LIMIT 100) m;

:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

RESET timescaledb.enable_chunk_append;

-- test prepared statement with params and generic plan
SET plan_cache_mode TO force_generic_plan;
PREPARE prep(timestamptz) AS SELECT device_id, time FROM :TEST_TABLE WHERE time = $1;

:PREFIX EXECUTE prep('2000-01-01 23:42');
:PREFIX EXECUTE prep('2000-01-10 23:42');
:PREFIX EXECUTE prep(now());

DEALLOCATE prep;
RESET plan_cache_mode;

