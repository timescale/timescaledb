-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timezone TO PST8PDT;

CREATE TABLE conditions (
  time         TIMESTAMP WITH TIME ZONE NOT NULL,
  device_id    TEXT,
  location_id  INTEGER,
  temperature  NUMERIC,
  humidity     NUMERIC
) WITH (
  timescaledb.hypertable,
  timescaledb.chunk_interval = '1 month'
);

INSERT INTO conditions
SELECT t, d::text, d, 1, 1 FROM generate_series('2025-12-15 00:00:00+00'::timestamptz - interval '1 year', '2025-12-15 00:00:00+00'::timestamptz, interval '1 hour') AS t, generate_series(1, 10) AS d;

CREATE MATERIALIZED VIEW conditions_hourly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(INTERVAL '1 hour', time) AS bucket,
  device_id,
  MAX(temperature),
  MIN(temperature),
  COUNT(*)
FROM conditions
GROUP BY 1, 2
WITH NO DATA;

-- Setting buckets_per_batch to a high value to bypass "disabling direct compress because of too small batch size" situation
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Enable columnstore
TRUNCATE conditions_hourly;
ALTER MATERIALIZED VIEW conditions_hourly SET (timescaledb.compress);

-- Enable direct compress on cagg refresh
SET timescaledb.enable_direct_compress_on_cagg_refresh TO on;
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Backfill data and refresh again WITHOUT direct compress
INSERT INTO conditions
SELECT t, d::text, 1, 1 FROM generate_series('2025-12-15 00:00:00+00'::timestamptz - interval '1 year', '2025-12-15 00:00:00+00'::timestamptz, interval '1 hour') AS t, generate_series(1, 10) AS d;
SET timescaledb.enable_direct_compress_on_cagg_refresh TO off;
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Recompress all uncompressed chunks
SELECT compress_chunk(show_chunks('conditions_hourly'), recompress => true) IS NOT NULL AS compress;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Backfill data and refresh again WITH direct compress
INSERT INTO conditions
SELECT t, d::text, 1, 1 FROM generate_series('2025-12-15 00:00:00+00'::timestamptz - interval '1 year', '2025-12-15 00:00:00+00'::timestamptz, interval '1 hour') AS t, generate_series(1, 10) AS d;
SET timescaledb.enable_direct_compress_on_cagg_refresh TO on;
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Cleanup
TRUNCATE conditions;
TRUNCATE conditions_hourly;

-- Hierarchical CAgg tests
CREATE MATERIALIZED VIEW conditions_daily
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(INTERVAL '1 day', bucket) AS bucket,
  device_id,
  MAX(max),
  MIN(min),
  SUM(count) AS count
FROM conditions_hourly
GROUP BY 1, 2
WITH NO DATA;

ALTER MATERIALIZED VIEW conditions_daily SET (timescaledb.compress);

INSERT INTO conditions
SELECT t, d::text, 1, 1 FROM generate_series('2025-12-15 00:00:00+00'::timestamptz - interval '1 year', '2025-12-15 00:00:00+00'::timestamptz, interval '1 hour') AS t, generate_series(1, 10) AS d;

SET timescaledb.enable_direct_compress_on_cagg_refresh TO on;

-- Refresh the base CAgg
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Refresh the hierarchical CAgg
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_daily') chunk;

-- Produce some invalidations for the base CAgg
INSERT INTO conditions
SELECT t, d::text, 1, 1 FROM generate_series('2025-12-15 00:00:00+00'::timestamptz - interval '1 year', '2025-12-15 00:00:00+00'::timestamptz, interval '1 hour') AS t, generate_series(1, 10) AS d;

-- Refresh the base CAgg
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Refreshing again the base CAgg is a no-op since everything is up to date
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);

-- Refresh the hierarchical CAgg with invalidations procuded by the base CAgg
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_daily') chunk;

-- Refreshing again the hierarchical CAgg is a no-op since everything is up to date
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);

-- Tests with custom segmentby and orderby
CREATE MATERIALIZED VIEW conditions_weekly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(INTERVAL '1 hour', time) AS bucket,
  location_id,
  device_id,
  MAX(temperature),
  MIN(temperature),
  COUNT(*)
FROM conditions
GROUP BY 1, 2, 3
WITH NO DATA;

ALTER MATERIALIZED VIEW conditions_weekly SET (timescaledb.compress_segmentby = 'device_id, location_id', timescaledb.compress_orderby = 'max, min, bucket DESC');

CALL refresh_continuous_aggregate('conditions_weekly', NULL, NULL, options => '{"buckets_per_batch": 10000}'::jsonb);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_weekly') chunk;

-- Test GROUP BY ROLLUP on compressed continuous aggregate (issue #9520)
SELECT bucket, MAX(max)
FROM conditions_hourly
WHERE bucket >= '2025-12-14 00:00:00+00'::timestamptz AND bucket < '2025-12-14 03:00:00+00'::timestamptz
GROUP BY ROLLUP(bucket)
ORDER BY bucket ASC NULLS FIRST;

RESET timescaledb.enable_direct_compress_on_cagg_refresh;

-- Tenant tracking on the direct-compress ingest path.
SET timezone TO 'UTC';
-- Anchor the granular-refresh start offset to a fixed date safely before all
-- the fixed fixture dates below, computed relative to today so the window keeps
-- covering them regardless of when this test actually runs.
SELECT (CURRENT_DATE - DATE '2019-01-01')::text || ' days' AS granular_refresh_lookback \gset

--  view over the tenant tracking catalog 
CREATE VIEW continuous_aggs_tenant_tracking_view AS
SELECT hypertable_id,
       tenant_id,
       _timescaledb_functions.to_timestamp(min_timestamp) AS min_timestamp,
       _timescaledb_functions.to_timestamp(max_timestamp) AS max_timestamp,
       seqnum
FROM _timescaledb_catalog.continuous_aggs_tenant_tracking;

CREATE TABLE tenant_conditions(time timestamptz NOT NULL, sensor_id text, value float)
  WITH (tsdb.hypertable, tsdb.orderby = 'time');
ALTER TABLE tenant_conditions SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 day'
);

CREATE MATERIALIZED VIEW tenant_daily
  WITH (timescaledb.continuous) AS
  SELECT time_bucket('1 day', time) AS bucket, sensor_id, avg(value)
  FROM tenant_conditions
  GROUP BY bucket, sensor_id
  WITH NO DATA;
ALTER MATERIALIZED VIEW tenant_daily SET (timescaledb.enable_granular_refresh = true);

SET timescaledb.enable_direct_compress_insert = true;
SET timescaledb.enable_direct_compress_insert_sort_batches = true;
SET timescaledb.enable_direct_compress_insert_client_sorted = false;

-- Direct-compress a batch of 2020 rows for three tenants (1000 rows each so the
-- batch is large enough to engage direct compress).  Each tenant has a single
-- distinct timestamp, so its tracked range is a point (min == max).
INSERT INTO tenant_conditions
SELECT v.ts, v.sensor, g
FROM (VALUES ('2020-01-01 00:00+00'::timestamptz, 'sensor_a'),
             ('2020-01-02 00:00+00'::timestamptz, 'sensor_b'),
             ('2020-01-03 00:00+00'::timestamptz, 'sensor_c')) v(ts, sensor),
     generate_series(1, 1000) g;

-- Confirm the direct-compress ingest path ran: the chunk is COMPRESSED.
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk)
FROM show_chunks('tenant_conditions') chunk;

RESET timescaledb.enable_direct_compress_insert;
RESET timescaledb.enable_direct_compress_insert_sort_batches;
RESET timescaledb.enable_direct_compress_insert_client_sorted;

-- A fresh 2025 row gives the refresh work to do so it drains the tracker.
-- Refreshing the 2025 window . All seqnum=1 entries for 2020 and 2025 can
-- be observed
INSERT INTO tenant_conditions VALUES ('2025-01-01 00:00+00', 'sensor_z', 0);
CALL refresh_continuous_aggregate('tenant_daily', '2025-01-01 00:00+00', NULL);

-- Expect sensor_a/sensor_b/sensor_c from the direct-compress INSERT, each with
-- min == max at its distinct day.
SELECT * FROM continuous_aggs_tenant_tracking_view
WHERE hypertable_id = (
    SELECT raw_hypertable_id FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_name = 'tenant_daily')
ORDER BY tenant_id, seqnum;

DROP MATERIALIZED VIEW tenant_daily;
DROP TABLE tenant_conditions;
DROP VIEW continuous_aggs_tenant_tracking_view;
