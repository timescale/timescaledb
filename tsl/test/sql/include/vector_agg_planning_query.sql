-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SHOW timescaledb.enable_vectorized_aggregation;

-- SubqueryScan
:EXPLAIN SELECT mx, mn, device FROM (SELECT device, count(time) mn, max(time) mx FROM metrics GROUP BY device) sub;

-- CteScan
:EXPLAIN WITH q1 AS MATERIALIZED (SELECT count(*) FROM metrics) SELECT * FROM q1;

-- InitPlan
:EXPLAIN SELECT FROM pg_class WHERE EXISTS (SELECT count(*) FROM metrics) LIMIT 1;

-- HAVING on aggregate without GROUP BY.
:EXPLAIN SELECT sum(value) from metrics HAVING sum(value) > 0;
:EXPLAIN SELECT sum(value) from metrics HAVING sum(value) < 0;

-- HAVING on aggregate with GROUP BY.
:EXPLAIN SELECT device, sum(value) from metrics GROUP BY device HAVING sum(value) > 500 ORDER BY device;
:EXPLAIN SELECT device, count(*) from metrics GROUP BY device HAVING count(*) > 100 ORDER BY device;

-- HAVING referencing a different aggregate than the target list.
:EXPLAIN SELECT device, sum(value) from metrics GROUP BY device HAVING count(*) > 100 ORDER BY device;
:EXPLAIN SELECT device, count(*) from metrics GROUP BY device HAVING sum(value) > 500 ORDER BY device;

-- HAVING with multiple conditions.
:EXPLAIN SELECT device, sum(value), count(*) from metrics GROUP BY device
    HAVING sum(value) > 500 and count(*) > 100 ORDER BY device;

-- HAVING on grouping column (pushed down by planner to WHERE).
:EXPLAIN SELECT device, sum(value) from metrics GROUP BY device HAVING device = 'dev 5' ORDER BY device;

-- HAVING with expressions on aggregates.
:EXPLAIN SELECT device, sum(value) from metrics GROUP BY device HAVING sum(value) * 2 > 1000 ORDER BY device;

-- HAVING with different aggregate functions.
:EXPLAIN SELECT device, min(value) from metrics GROUP BY device HAVING min(value) = 0 ORDER BY device;
:EXPLAIN SELECT device, max(value) from metrics GROUP BY device HAVING max(value) < 90 ORDER BY device;
:EXPLAIN SELECT device, avg(value) from metrics GROUP BY device HAVING avg(value) > 49 ORDER BY device;

-- HAVING with segmentby grouping.
:EXPLAIN SELECT device, sum(value) from metrics GROUP BY device HAVING sum(value) > 10000 ORDER BY device;

-- Expression over a grouping column in the select list.
:EXPLAIN SELECT value + 1 AS vp1, sum(value) FROM metrics GROUP BY value ORDER BY vp1;

-- Non-injective expression over a grouping column, without the bare
-- column in the select list.
:EXPLAIN SELECT floor(value / 10) AS vf, sum(value) AS s FROM metrics GROUP BY value ORDER BY vf, s;

-- Expression over a text grouping column, collapsing all groups to one value.
:EXPLAIN SELECT length(device) AS ld, sum(value) AS s FROM metrics GROUP BY device ORDER BY ld, s;

-- Constant in the select list of a grouped aggregate.
:EXPLAIN SELECT 42 AS c, value, sum(value) FROM metrics GROUP BY value ORDER BY value;

-- Stable expression without column references in the select list.
:EXPLAIN SELECT current_setting('timezone') AS tz, sum(value) FROM metrics;

-- Correlated Subquery on single chunk
:EXPLAIN SELECT FROM (SELECT 12 AS c7 FROM pg_class LIMIT 1) AS subq_1 WHERE EXISTS(SELECT FROM _timescaledb_internal._hyper_1_1_chunk WHERE subq_1.c7 = CASE WHEN value = 1 THEN NULL::int2 END);

-- Correlated subquery deduplicated on two expressions: a grouping
-- without a GROUP BY clause, whose grouping expressions appear in no
-- select list.
:EXPLAIN SELECT FROM (SELECT 12 AS c7, 13 AS c8 FROM pg_class LIMIT 1) AS subq_1 WHERE EXISTS(SELECT FROM _timescaledb_internal._hyper_1_1_chunk WHERE subq_1.c7 = CASE WHEN value = 1 THEN NULL::int2 END AND subq_1.c8 = CASE WHEN value = 2 THEN NULL::int2 END);

-- Expression over an aggregate in the select list.
:EXPLAIN SELECT sum(value) + 1 AS sp1 FROM metrics GROUP BY value ORDER BY sp1;

-- Expression over both a grouping column and an aggregate.
:EXPLAIN SELECT value + sum(value) AS vs FROM metrics GROUP BY value ORDER BY vs;

-- Duplicate identical aggregates.
:EXPLAIN SELECT sum(value), sum(value), value FROM metrics GROUP BY value ORDER BY value;

-- Aggregate listed before the grouping column in the select list.
:EXPLAIN SELECT sum(value) AS s, device FROM metrics GROUP BY device ORDER BY device;

-- Aggregate whose partial state type differs from its result type.
:EXPLAIN SELECT value, avg(value) FROM metrics GROUP BY value ORDER BY value;

-- Grouping by a system column is not vectorized.
:EXPLAIN SELECT tableoid::regclass::text AS chunk, count(*) FROM metrics GROUP BY tableoid ORDER BY chunk;

-- Volatile expression over a grouping column in the select list.
:EXPLAIN SELECT value + random() * 0 AS vr, sum(value) FROM metrics GROUP BY value ORDER BY vr;

-- Expressions over a grouping column computed by the finalize
-- aggregate.
:EXPLAIN SELECT coalesce(value, 0) AS cv, sum(value) FROM metrics GROUP BY value ORDER BY cv;
:EXPLAIN SELECT CASE value WHEN 0 THEN -1 ELSE value END AS sw, sum(value) FROM metrics GROUP BY value ORDER BY sw;

-- Volatile grouping expression is not vectorized.
:EXPLAIN SELECT sum(value) FROM metrics GROUP BY value + (random() * 0)::int;

-- Volatile aggregate argument is not vectorized.
:EXPLAIN SELECT sum(value + (random() * 0)::int) FROM metrics GROUP BY value;

-- Volatile aggregate filter is not vectorized.
:EXPLAIN SELECT sum(value) FILTER (WHERE random() < 2) FROM metrics GROUP BY value ORDER BY value;

-- Volatile expression over an aggregate, computed once per group.
:EXPLAIN SELECT sum(value) * (1 + random() * 0) AS sr FROM metrics GROUP BY value ORDER BY sr;
