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

CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Enable columnstore
TRUNCATE conditions_hourly;
ALTER MATERIALIZED VIEW conditions_hourly SET (timescaledb.compress);

-- Enable direct compress on cagg refresh
SET timescaledb.enable_direct_compress_on_cagg_refresh TO on;
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Backfill data and refresh again WITHOUT direct compress
INSERT INTO conditions
SELECT t, d::text, 1, 1 FROM generate_series('2025-12-15 00:00:00+00'::timestamptz - interval '1 year', '2025-12-15 00:00:00+00'::timestamptz, interval '1 hour') AS t, generate_series(1, 10) AS d;
SET timescaledb.enable_direct_compress_on_cagg_refresh TO off;
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Recompress all uncompressed chunks
SELECT compress_chunk(show_chunks('conditions_hourly'), recompress => true) IS NOT NULL AS compress;
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Backfill data and refresh again WITH direct compress
INSERT INTO conditions
SELECT t, d::text, 1, 1 FROM generate_series('2025-12-15 00:00:00+00'::timestamptz - interval '1 year', '2025-12-15 00:00:00+00'::timestamptz, interval '1 hour') AS t, generate_series(1, 10) AS d;
SET timescaledb.enable_direct_compress_on_cagg_refresh TO on;
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL);
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
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Refresh the hierarchical CAgg
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_daily') chunk;

-- Produce some invalidations for the base CAgg
INSERT INTO conditions
SELECT t, d::text, 1, 1 FROM generate_series('2025-12-15 00:00:00+00'::timestamptz - interval '1 year', '2025-12-15 00:00:00+00'::timestamptz, interval '1 hour') AS t, generate_series(1, 10) AS d;

-- Refresh the base CAgg
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_hourly') chunk;

-- Refreshing again the base CAgg is a no-op since everything is up to date
CALL refresh_continuous_aggregate('conditions_hourly', NULL, NULL);

-- Refresh the hierarchical CAgg with invalidations procuded by the base CAgg
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_daily') chunk;

-- Refreshing again the hierarchical CAgg is a no-op since everything is up to date
CALL refresh_continuous_aggregate('conditions_daily', NULL, NULL);

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

CALL refresh_continuous_aggregate('conditions_weekly', NULL, NULL);
SELECT DISTINCT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('conditions_weekly') chunk;

RESET timescaledb.enable_direct_compress_on_cagg_refresh;
