-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (costs off)'
-- we disable ChunkAppend and ConstraintAwareAppend here to make the exclusion easier to spot
-- otherwise those would remove the chunks from the plan during execution

SET timescaledb.enable_chunk_append TO FALSE;

SET timescaledb.enable_constraint_aware_append TO FALSE;

-- plan query on complete hypertable to get a list of the chunks
:PREFIX
SELECT time
FROM metrics;

-- all of these should have all chunk exclusion happening at plan time
:PREFIX
SELECT time
FROM metrics
WHERE time < '2000-01-01'::timestamptz - '6h'::interval;

:PREFIX
SELECT time
FROM metrics
WHERE time < '2000-01-01'::timestamptz + '6h'::interval;

:PREFIX
SELECT time
FROM metrics
WHERE time < '6h'::interval + '2000-01-01'::timestamptz;

:PREFIX
SELECT time
FROM metrics
WHERE time < '2000-01-07'::timestamptz - '7 day 8 seconds'::interval;

:PREFIX
SELECT time
FROM metrics
WHERE time < '2000-03-01'::timestamptz - '60 day'::interval;

-- test Var on right side of expression
:PREFIX
SELECT time
FROM metrics
WHERE '2000-01-01'::timestamptz - '6h'::interval > time;

:PREFIX
SELECT time
FROM metrics
WHERE '2000-01-07'::timestamptz - '7 day'::interval > time;

:PREFIX
SELECT time
FROM metrics
WHERE '2000-03-01'::timestamptz - '60 day'::interval > time;

-- test multiple constraints
:PREFIX
SELECT time
FROM metrics
WHERE time > '2000-01-10'::timestamptz - '6h'::interval
    AND time < '2000-01-10'::timestamptz + '6h'::interval;

-- test on space-partitioned hypertable
:PREFIX
SELECT time
FROM metrics_space
WHERE time < '2000-01-01'::timestamptz - '6h'::interval
    AND device_id = 1;

-- test on compressed hypertable
:PREFIX
SELECT time
FROM metrics_compressed
WHERE time < '2000-01-01'::timestamptz - '6h'::interval;

-- test on space-partitioned compressed hypertable
:PREFIX
SELECT time
FROM metrics_space_compressed
WHERE time < '2000-01-01'::timestamptz - '6h'::interval
    AND device_id = 1;

-- month/year intervals are not constified
:PREFIX
SELECT time
FROM metrics
WHERE time < '2000-02-01'::timestamptz - '1 month'::interval;

:PREFIX
SELECT time
FROM metrics
WHERE time < '2000-02-01'::timestamptz - '1 month - 1 day'::interval;

:PREFIX
SELECT time
FROM metrics
WHERE time < '2000-02-01'::timestamptz - '1 month + 1 day'::interval;

:PREFIX
SELECT time
FROM metrics
WHERE '2000-02-01'::timestamptz - '1 year'::interval > time;

-- nested expressions are not constified
:PREFIX
SELECT time
FROM metrics
WHERE time < '1 day' + '2000-02-01'::timestamptz - '1 month'::interval;

-- non-Const expressions are not constified
:PREFIX
SELECT time
FROM metrics
WHERE time > now() - '6h'::interval;

-- test NULL values
:PREFIX
SELECT time
FROM metrics
WHERE time < '2000-02-01'::timestamptz - NULL::interval;

:PREFIX
SELECT time
FROM metrics
WHERE time < NULL::timestamptz - NULL::interval;

