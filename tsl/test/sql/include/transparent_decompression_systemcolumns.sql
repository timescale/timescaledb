-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set TEST_TABLE 'metrics'

-- test system columns
-- all system columns except for tableoid should error
\set ON_ERROR_STOP 0
SELECT xmin FROM :TEST_TABLE ORDER BY time;
SELECT cmin FROM :TEST_TABLE ORDER BY time;
SELECT xmax FROM :TEST_TABLE ORDER BY time;
SELECT cmax FROM :TEST_TABLE ORDER BY time;
SELECT ctid FROM :TEST_TABLE ORDER BY time;

-- test system columns in WHERE and ORDER BY clause
SELECT tableoid, xmin FROM :TEST_TABLE ORDER BY time;
SELECT FROM :TEST_TABLE ORDER BY cmin::text;
SELECT FROM :TEST_TABLE WHERE cmin IS NOT NULL;
\set ON_ERROR_STOP 1

-- test tableoid in different parts of query
SELECT pg_typeof(tableoid) FROM :TEST_TABLE ORDER BY time LIMIT 1;
SELECT FROM :TEST_TABLE ORDER BY tableoid LIMIT 1;
SELECT FROM :TEST_TABLE WHERE tableoid::int > 0 LIMIT 1;
SELECT tableoid::regclass FROM :TEST_TABLE GROUP BY tableoid ORDER BY 1;
SELECT count(distinct tableoid) FROM :TEST_TABLE WHERE device_id=1 AND time < now();

-- test prepared statement
PREPARE tableoid_prep AS SELECT tableoid::regclass FROM :TEST_TABLE WHERE device_id = 1 ORDER BY time LIMIT 1;
:PREFIX EXECUTE tableoid_prep;
EXECUTE tableoid_prep;
EXECUTE tableoid_prep;
EXECUTE tableoid_prep;
EXECUTE tableoid_prep;
EXECUTE tableoid_prep;
DEALLOCATE tableoid_prep;

