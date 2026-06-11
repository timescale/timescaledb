-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Error / out-of-shared-memory fall back for the per-tenant invalidation tracker.
--

SET timezone TO 'UTC';

-- Anchor the start offset to a fixed date safely before all the fixed 2020
-- fixture dates below, computed relative to today so the window keeps
-- covering them regardless of when this test actually runs.
SELECT (CURRENT_DATE - DATE '2019-01-01')::text || ' days' AS granular_refresh_lookback \gset

CREATE TABLE conditions(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('conditions', 'time');
ALTER TABLE conditions SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW cond_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM conditions
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW cond_daily SET (timescaledb.enable_granular_refresh = true);

-- ============================================================================
-- Part 1: Out of shared memory during per-hypertable tracker allocation
-- ============================================================================

-- Force the hypertable tracker allocation to OOM . so new hypertable tracking
-- will not work
SELECT debug_waitpoint_enable('tenant_tracker_area_oom');

-- (i) The INSERT commits even though the tracker allocation failed.
INSERT INTO conditions VALUES
  ('2020-01-01 00:00+00', 'sensor_a', 1),
  ('2020-01-02 00:00+00', 'sensor_a', 2),
  ('2020-01-01 00:00+00', 'sensor_b', 3);

-- (ii) A second INSERT into the same hypertable does NOT re-attempt the
-- allocation
INSERT INTO conditions VALUES ('2020-01-02 12:00+00', 'sensor_b', 4);

-- all 4 rows were inserted.
SELECT COUNT(*) FROM conditions;

-- (iii) Nothing was tracked for this hypertable. if it was tracked, it does
-- not get processed by this refresh and would be written out.
CALL refresh_continuous_aggregate('cond_daily', '2025-05-01 00:00+00', NULL);
SELECT count(*) AS tracking_rows_during_oom
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'cond_daily');

-- (iv) The refresh falls back to the full invalidation log and produces the
-- correct result for every bucket.
CALL refresh_continuous_aggregate('cond_daily', NULL, NULL);

SELECT sensor_id, bucket, avg
FROM cond_daily
ORDER BY sensor_id, bucket;

-- Clear the injection.  There is no allocation for this hypertable until a
-- restart
SELECT debug_waitpoint_release('tenant_tracker_area_oom');

INSERT INTO conditions VALUES ('2020-01-03 00:00+00', 'sensor_a', 5);
INSERT INTO conditions VALUES ('2025-05-03 00:00+00', 'sensor_b', 5);
CALL refresh_continuous_aggregate('cond_daily', '2025-05-01 00:00+00', NULL);

SELECT count(*) AS tracking_rows_after_release
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'cond_daily');

-- refresh still works
CALL refresh_continuous_aggregate('cond_daily', NULL, NULL);
SELECT sensor_id, bucket, avg
FROM cond_daily
ORDER BY sensor_id, bucket;

-- ============================================================================
-- Part 2: a OOM from dshash_find_or_insert
-- ============================================================================

CREATE TABLE metrics(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('metrics', 'time');
ALTER TABLE metrics SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW metric_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM metrics
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW metric_daily SET (timescaledb.enable_granular_refresh = true);

-- Make get_or_attach throw with the dshash partition lock held.
SELECT debug_waitpoint_enable('tenant_tracker_map_dshash_insert_oom');

INSERT INTO metrics VALUES ('2020-01-01 00:00+00', 'sensor_x', 10);

-- (v) The INSERT commits: no errors thrown. skips tracking for txn
INSERT INTO metrics VALUES ('2020-01-01 06:00+00', 'sensor_x', 20);

-- (vi) same with next insert.
INSERT INTO metrics VALUES ('2020-01-01 12:00+00', 'sensor_x', 30);

-- All three rows committed and no tracking entries.
SELECT count(*) AS metrics_rows FROM metrics;

SELECT debug_waitpoint_release('tenant_tracker_map_dshash_insert_oom');

-- refresh will force any tracking entries outside range to get written out.
CALL refresh_continuous_aggregate('metric_daily', '2025-03-01 10:00+00', NULL);
SELECT *
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metric_daily');

-- refresh still works as we use invalidation logs
CALL refresh_continuous_aggregate('metric_daily', '2020-01-01 00:00+00', NULL);
SELECT sensor_id, bucket, avg
FROM metric_daily
ORDER BY sensor_id, bucket;

--Tracking won't recover until restart, so we will not see any new tracking entries.

INSERT INTO metrics VALUES ('2020-01-02 00:00+00', 'sensor_x', 40);
INSERT INTO metrics VALUES ('2025-05-05 00:00+00', 'sensor_y', 50);

CALL refresh_continuous_aggregate('metric_daily', '2025-02-01 10:00+00', NULL);
SELECT *
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'metric_daily');
