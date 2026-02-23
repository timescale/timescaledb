-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET datestyle TO 'ISO, YMD';
SET timezone TO 'UTC';

\set PREFIX 'EXPLAIN (buffers off, costs off, timing off, summary off)'

CREATE TABLE metrics(time timestamptz NOT NULL, device_id int, value float);
SELECT table_name FROM create_hypertable('metrics', 'time', chunk_time_interval => INTERVAL '7 days');

INSERT INTO metrics(time, value, device_id) VALUES
  ('2021-06-14', 26,1),
  ('2021-06-15', 22,2),
  ('2021-06-15', 21,1),
  ('2021-06-16', 24,3),
  ('2021-06-20', 24,4),
  ('2021-06-18', 27,4),
  ('2021-06-19', 28,4),
  ('2021-06-25', 30,1),
  ('2021-06-21', 31,1),
  ('2021-06-22', 34,1),
  ('2021-06-22', 34,2),
  ('2021-06-24', 34,2),
  ('2021-06-25', 32,3),
  ('2021-06-26', 32,3),
  ('2021-06-15', 23,1),
  ('2021-06-17', 24,2),
  ('2021-06-17', 21,3),
  ('2021-06-27', 31,3);

-- For stable plans
SET enable_indexscan = 0;
SET enable_bitmapscan = 0;

:PREFIX SELECT * from metrics;

-- check default view for new continuous aggregate
CREATE MATERIALIZED VIEW metrics_realtime
  WITH (timescaledb.continuous, timescaledb.materialized_only=false)
AS
  SELECT time_bucket('1d',time) as bucket, avg(value) as av FROM metrics GROUP BY 1 WITH NO DATA;

CREATE MATERIALIZED VIEW metrics_realtime_h
  WITH (timescaledb.continuous, timescaledb.materialized_only=false)
AS
  SELECT time_bucket('2d', bucket), avg(av) FROM metrics_realtime GROUP BY 1 WITH NO DATA;

CREATE MATERIALIZED VIEW metrics_matonly
  WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
  SELECT time_bucket('1d',time) as bucket, avg(value) as av FROM metrics GROUP BY 1 WITH NO DATA;

-- Check settings for "realtime_cagg_settings"

-- Caggs with no materialized data
RESET timescaledb.realtime_cagg_settings;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

SET timescaledb.realtime_cagg_settings = materialized_only;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

SET timescaledb.realtime_cagg_settings = realtime;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

SET timescaledb.realtime_cagg_settings = realtime_with_backfills;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

-- Caggs with entire data materialized
CALL refresh_continuous_aggregate('metrics_realtime', NULL, NULL);
CALL refresh_continuous_aggregate('metrics_realtime_h', NULL, NULL);
CALL refresh_continuous_aggregate('metrics_matonly', NULL, NULL);

RESET timescaledb.realtime_cagg_settings;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

SET timescaledb.realtime_cagg_settings = materialized_only;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

SET timescaledb.realtime_cagg_settings = realtime;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

SET timescaledb.realtime_cagg_settings = realtime_with_backfills;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

-- Add new data above watermark
INSERT INTO metrics(time, value, device_id) VALUES
  ('2021-06-30', 23,1),
  ('2021-06-30', 25,2),
  ('2021-07-01', 30,1);

-- Refresh "metrics_realtime", leave "metrics_realtime_h" unrefreshed
CALL refresh_continuous_aggregate('metrics_realtime', NULL, NULL);

SET timescaledb.realtime_cagg_settings = realtime;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;

SET timescaledb.realtime_cagg_settings = realtime_with_backfills;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;

-- Backfill some data
INSERT INTO metrics(time, value, device_id) VALUES
  ('2021-06-15', 22,1),
  ('2021-06-15', 23,2),
  ('2021-06-20', 25,3),
  ('2021-06-20', 30,4);

SET timescaledb.realtime_cagg_settings = realtime;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

SET timescaledb.realtime_cagg_settings = realtime_with_backfills;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

-- Push invalidated ranges into cagg tables
CALL refresh_continuous_aggregate('metrics_matonly', NULL, NULL);

SET timescaledb.realtime_cagg_settings = realtime_with_backfills;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

-- Allow only 1 invalidated range to be included: should revert to "realtime" mode from "realtime_with_backfills"
SET timescaledb.cagg_max_individual_materializations = 1;

SET timescaledb.realtime_cagg_settings = realtime_with_backfills;
:PREFIX SELECT * from metrics_realtime;
:PREFIX SELECT * from metrics_realtime_h;
:PREFIX SELECT * from metrics_matonly;

RESET timescaledb.realtime_cagg_settings;
DROP MATERIALIZED VIEW metrics_realtime_h cascade;
DROP MATERIALIZED VIEW metrics_realtime cascade;
DROP MATERIALIZED VIEW metrics_matonly cascade;

DROP TABLE metrics cascade;
