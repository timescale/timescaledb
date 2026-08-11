-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Correctness when per-tenant tracking degrades to a full refresh.
--
-- The tracker is an optimization layered over the invalidation log: an
-- overflowed generation, an unstorable tenant key, or tracking turned off must
-- all fall back to a full refresh that still produces the exact same result.
-- Tenants are tracked only for late-arriving data (older than now - 1 day), so
-- all data here uses fixed 2020 timestamps.

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
-- Overflow: more than TT_CAPACITY * 3/4 (>= 3073) distinct late tenants in one
-- generation trips it INVALID.  The flush writes a marker instead of per-tenant
-- rows, so the refresh falls back to a full refresh -- which must still
-- materialize every one of the 4000 sensors correctly.
-- ============================================================================
INSERT INTO conditions
  SELECT '2020-01-01 00:00+00', 'sensor_' || g, g
  FROM generate_series(1, 4000) g;

-- insert so that refresh has something to do.  refresh will also flush shared mem emtries
INSERT INTO conditions VALUES ( '2025-03-01 10:00+00', 'sensor_b', 20);
CALL refresh_continuous_aggregate('cond_daily', '2025-03-01 00:00+00', NULL);

-- Note that before the above refresh, invalidation threshold was set as min int64,
-- So the inserts doesn't write invalidations. Therefore, although the invalid marker
-- is written, it does not have an associated invalidation in the invalidation log.
--invalid marker is cleaned up after refresh because there are no more invalidations
--with the same seqnum, so we don't see it here.
SELECT *
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'cond_daily');

--now refresh the actual range, should fallback to regular refresh
CALL refresh_continuous_aggregate('cond_daily', '2020-01-01 00:00+00', NULL);
-- Every distinct sensor is materialized; each bucket has a single row so its
-- average equals the inserted value (the numeric suffix of the sensor name).
SELECT count(*) AS materialized_sensors,
       count(*) FILTER (WHERE avg <> expected) AS wrong_averages
FROM (
  SELECT avg, split_part(sensor_id, '_', 2)::float AS expected
  FROM cond_daily
  WHERE bucket = '2020-01-01 00:00+00'
) s;

-- ============================================================================
-- Unstorable key: a tenant key longer than TT_KEY_MAXLEN (64 bytes) also trips
-- the generation INVALID, taking the whole interval down the same full-refresh
-- fall back -- so the long-keyed sensor and a normal one in the same interval
-- are both materialized correctly.
-- ============================================================================
INSERT INTO conditions VALUES
  ('2020-02-01 00:00+00', repeat('x', 100), 10),
  ('2020-02-01 00:00+00', 'short', 20);

-- will force write out of invalid seqnum entry
INSERT INTO conditions VALUES ( '2025-01-01 10:00+00', 'sensor_b', 20);
CALL refresh_continuous_aggregate('cond_daily', '2025-01-01 00:00+00', NULL);

SELECT *
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'cond_daily');

--refresh still works by using invalidation logs
CALL refresh_continuous_aggregate('cond_daily', '2020-02-01 00:00+00', NULL);
SELECT length(sensor_id) AS key_len, avg
FROM cond_daily
WHERE bucket = '2020-02-01 00:00+00'
ORDER BY key_len;

-- A NULL tenant is unstorable and trips the generation INVALID (status 1).  The
-- whole transaction is skipped, so nentries stays 0 even though 'named' is storable.
INSERT INTO conditions VALUES
  ('2020-05-01 00:00+00', 'named', 40),
  ('2020-05-01 00:00+00', NULL, 50);
SELECT nentries, status
FROM _timescaledb_functions.hypertable_get_tenant_tracking_info('conditions'::regclass);

-- Both groups materialize via the fall back.
CALL refresh_continuous_aggregate('cond_daily', '2020-05-01 00:00+00', '2020-05-02 00:00+00');
SELECT sensor_id, avg
FROM cond_daily
WHERE bucket = '2020-05-01 00:00+00'
ORDER BY sensor_id NULLS LAST;

-- ============================================================================
-- Tracking not configured: a hypertable with no granular refresh
-- configuration skips the collection hook and granular filter entirely, so
-- the refresh is always full and still correct.
-- ============================================================================
CREATE TABLE conditions_untracked(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('conditions_untracked', 'time');

CREATE MATERIALIZED VIEW cond_untracked_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM conditions_untracked
  GROUP BY bucket, sensor_id
  WITH NO DATA;

INSERT INTO conditions_untracked VALUES
  ('2020-03-01 00:00+00', 'sensor_a', 1),
  ('2020-03-01 00:00+00', 'sensor_a', 3),
  ('2020-03-01 00:00+00', 'sensor_b', 5);

CALL refresh_continuous_aggregate('cond_untracked_daily', NULL, NULL);

SELECT sensor_id, avg
FROM cond_untracked_daily
WHERE bucket = '2020-03-01 00:00+00'
ORDER BY sensor_id;

-- No per-tenant rows are left behind: a granular refresh consumes them, and the
-- fall-back paths above never wrote any (they recorded invalid markers instead).
SELECT count(*) AS leftover_tenant_rows
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'cond_daily')
  AND tenant_id IS NOT NULL;

-- The two INVALID flushes (overflow, unstorable key) each persisted an invalid
-- marker (tenant_id NULL).  The seqnum-aware cleanup during refresh removes a
-- marker once its seqnum's cagg invalidation has been consumed, so none remain.
SELECT count(*) AS leftover_marker_rows
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'cond_daily')
  AND tenant_id IS NULL;

DROP MATERIALIZED VIEW cond_untracked_daily;
DROP MATERIALIZED VIEW cond_daily;
DROP TABLE conditions_untracked;
DROP TABLE conditions;

-- ============================================================================
-- Outside the late-arrival window: an invalidation entry disjoint from the
-- window has no tracking entries behind it, so its seqnum is NULL and the
-- refresh falls back to the full log.  Uses its own hypertable with the window
-- placed in the past, so both the older and the newer side of it can be hit
-- with fixed timestamps that still land below the invalidation threshold.
-- ============================================================================
CREATE TABLE readings(time timestamptz NOT NULL, sensor_id text, value float);
SELECT create_hypertable('readings', 'time');

-- Offsets computed from today so the window stays [2021-01-01, 2023-01-01)
-- regardless of when this test runs.
SELECT (CURRENT_DATE - DATE '2021-01-01')::text || ' days' AS window_start_offset,
       (CURRENT_DATE - DATE '2023-01-01')::text || ' days' AS window_end_offset \gset
ALTER TABLE readings SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'window_start_offset',
    timescaledb.granular_refresh_end_offset = :'window_end_offset'
);

CREATE MATERIALIZED VIEW readings_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM readings
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW readings_daily SET (timescaledb.enable_granular_refresh = true);

-- Refresh first so the invalidation threshold is above all three timestamps
-- below and every insert actually lands in the invalidation log.
CALL refresh_continuous_aggregate('readings_daily', NULL, '2026-01-01 00:00+00');

-- One transaction each: before the window, inside it, after it.
INSERT INTO readings VALUES ('2020-06-01 00:00+00', 'before_window', 1);
INSERT INTO readings VALUES ('2022-06-01 00:00+00', 'inside_window', 2);
INSERT INTO readings VALUES ('2024-06-01 00:00+00', 'after_window', 3);

-- Only the inside-window entry carries a seqnum.  The other two have a NULL
-- seqnum, expected because they are outside the late arrival window
-- [2021-01-01, 2023-01-01) so nothing was tracked for them.
SELECT hypertable_id,
       _timescaledb_functions.to_timestamp(lowest_modified_value) AS "start",
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS "end",
       seqnum
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'readings_daily')
ORDER BY greatest_modified_value;

-- All three are materialized correctly: the inside-window one via the granular
-- path, the other two via the full-log fall back.
CALL refresh_continuous_aggregate('readings_daily', NULL, '2026-01-01 00:00+00');
SELECT sensor_id, avg
FROM readings_daily
ORDER BY sensor_id;

DROP MATERIALIZED VIEW readings_daily;
DROP TABLE readings;
