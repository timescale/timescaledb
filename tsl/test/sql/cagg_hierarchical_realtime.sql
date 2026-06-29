-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Regression test for #10071.
--
-- A real-time continuous aggregate (timescaledb.materialized_only = false) that
-- is three or more levels deep in a hierarchy (a cagg on a cagg on a cagg) must
-- prune its real-time branch when refreshed for a window entirely below the
-- watermark, exactly as a two-level cagg does. A refresh reads the cagg's
-- internal partial view, so EXPLAINing a below-watermark SELECT on that partial
-- view reproduces the refresh scan. Before the fix the real-time branch was
-- pruned only up to two levels; at three or more it re-scanned the whole
-- un-materialized tail of the base hypertable and discarded every row.

SET timezone TO PST8PDT;
SET max_parallel_workers_per_gather TO 0;

CREATE TABLE conditions (ts timestamptz NOT NULL, value double precision NOT NULL);
SELECT create_hypertable('conditions', by_range('ts', INTERVAL '7 days'));

INSERT INTO conditions
SELECT ts, 1.0
FROM generate_series(TIMESTAMPTZ '2026-01-01', TIMESTAMPTZ '2026-04-29', INTERVAL '1 hour') AS ts;

-- Level 1: 6-hour buckets on the raw hypertable
CREATE MATERIALIZED VIEW cagg_l1
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('6 hours', ts) AS ts, count(*) AS cnt, sum(value) AS sum_value
FROM conditions GROUP BY 1 WITH NO DATA;

-- Level 2: 1-day buckets on cagg_l1 (raw -> 6h -> 1day)
CREATE MATERIALIZED VIEW cagg_l2
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 day', ts) AS ts, sum(cnt) AS cnt, sum(sum_value) AS sum_value
FROM cagg_l1 GROUP BY 1 WITH NO DATA;

-- Level 3: 7-day buckets on cagg_l2 (raw -> 6h -> 1day -> 7day)
CREATE MATERIALIZED VIEW cagg_l3
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('7 days', ts) AS ts, sum(cnt) AS cnt, sum(sum_value) AS sum_value
FROM cagg_l2 GROUP BY 1 WITH NO DATA;

-- Level 4: 28-day buckets on cagg_l3 (raw -> 6h -> 1day -> 7day -> 28day)
CREATE MATERIALIZED VIEW cagg_l4
  WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('28 days', ts) AS ts, sum(cnt) AS cnt, sum(sum_value) AS sum_value
FROM cagg_l3 GROUP BY 1 WITH NO DATA;

-- Materialize everything up to 2026-04-01, leaving an un-materialized tail.
CALL refresh_continuous_aggregate('cagg_l1', NULL, TIMESTAMPTZ '2026-04-01');
CALL refresh_continuous_aggregate('cagg_l2', NULL, TIMESTAMPTZ '2026-04-01');
CALL refresh_continuous_aggregate('cagg_l3', NULL, TIMESTAMPTZ '2026-04-01');
CALL refresh_continuous_aggregate('cagg_l4', NULL, TIMESTAMPTZ '2026-04-01');

-- A refresh reads the cagg's internal partial view, so EXPLAINing a
-- below-watermark SELECT on that partial view reproduces the refresh scan. Look
-- up each partial view name.
SELECT format('%I.%I', partial_view_schema, partial_view_name) AS pv_l2
FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'cagg_l2' \gset
SELECT format('%I.%I', partial_view_schema, partial_view_name) AS pv_l3
FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'cagg_l3' \gset
SELECT format('%I.%I', partial_view_schema, partial_view_name) AS pv_l4
FROM _timescaledb_catalog.continuous_agg WHERE user_view_name = 'cagg_l4' \gset

\set PREFIX 'EXPLAIN (COSTS OFF, TIMING OFF, SUMMARY OFF)'

-- For a window entirely below the watermark the real-time branch must be pruned
-- at every depth: the plan should fold it to "One-Time Filter: false" and touch
-- only the materialized hypertable, never the base-hypertable (_hyper_1_*)
-- chunks.

-- 2 levels (control: pruned before and after the fix)
:PREFIX SELECT * FROM :pv_l2 WHERE ts >= TIMESTAMPTZ '2026-01-13' AND ts < TIMESTAMPTZ '2026-02-10';

-- 3 levels (the bug: real-time branch must be pruned)
:PREFIX SELECT * FROM :pv_l3 WHERE ts >= TIMESTAMPTZ '2026-01-13' AND ts < TIMESTAMPTZ '2026-02-10';

-- 4 levels
:PREFIX SELECT * FROM :pv_l4 WHERE ts >= TIMESTAMPTZ '2026-01-13' AND ts < TIMESTAMPTZ '2026-02-10';

DROP TABLE conditions CASCADE;
