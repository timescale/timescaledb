-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test that refresh works even when migrating between invalidation
-- methods.
--
-- Since inserts into the hypertable ends up in either the WAL or the
-- hypertable invalidation log table, we check that moving between
-- them actually does what is expected and move all invalidations to
-- the materialization log.

CREATE VIEW hypertable_invalidation_thresholds AS
SELECT format('%I.%I', ht.schema_name, ht.table_name)::regclass AS hypertable,
       _timescaledb_functions.to_timestamp_without_timezone(watermark) AS threshold
  FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
  JOIN _timescaledb_catalog.hypertable ht
    ON hypertable_id = ht.id;

CREATE VIEW materialization_invalidations AS
SELECT ca.user_view_name AS aggregate_name,
       ht.table_name,
       _timescaledb_functions.to_timestamp_without_timezone(lowest_modified_value) AS lowest,
       _timescaledb_functions.to_timestamp_without_timezone(greatest_modified_value) AS greatest
  FROM _timescaledb_catalog.continuous_agg ca
  JOIN _timescaledb_catalog.continuous_aggs_materialization_invalidation_log ml
    ON ca.mat_hypertable_id = ml.materialization_id
  JOIN _timescaledb_catalog.hypertable ht
    ON materialization_id = ht.id
 WHERE lowest_modified_value BETWEEN 0 AND 1759302000000000
   AND greatest_modified_value BETWEEN 0 AND 1759302000000000;

CREATE TABLE device_readings (
    created_at timestamp NOT NULL,
    device_id text NOT NULL,
    metric double precision NOT NULL,
    PRIMARY KEY (device_id, created_at)
);

SELECT table_name FROM create_hypertable ('device_readings', 'created_at');

CREATE MATERIALIZED VIEW device_summary_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT
    time_bucket ('1 hour', created_at) AS bucket,
    device_id,
    sum(metric) AS metric_sum,
    max(metric) - min(metric) AS metric_spread
FROM
    device_readings
GROUP BY
    bucket,
    device_id
WITH NO DATA;

CREATE MATERIALIZED VIEW device_summary_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT
    time_bucket ('1 day', created_at) AS bucket,
    device_id,
    sum(metric) AS metric_sum,
    max(metric) - min(metric) AS metric_spread
FROM
    device_readings
GROUP BY
    bucket,
    device_id
WITH NO DATA;

-- Shall be empty
SELECT * FROM device_summary_hourly;
SELECT * FROM device_summary_daily;

-- Call refresh to set the threshold correctly.
CALL refresh_continuous_aggregate('device_summary_hourly', NULL, '2025-10-01');
CALL refresh_continuous_aggregate('device_summary_daily', NULL, '2025-10-01');
SELECT * FROM hypertable_invalidation_thresholds ORDER BY 1,2;

-- Insert something before the threshold, run refresh, and check the result
INSERT INTO device_readings VALUES
  ('2025-09-10 12:34:56', 1, 1),
  ('2025-09-10 12:34:57', 1, 2);
CALL refresh_continuous_aggregate('device_summary_hourly', NULL, '2025-10-01');
CALL refresh_continuous_aggregate('device_summary_daily', NULL, '2025-10-01');
SELECT * FROM device_summary_hourly;
SELECT * FROM device_summary_daily;

--
-- Test that we can migrate to use WAL without losing any invalidations.
--

-- Now insert new values in the hypertable
INSERT INTO device_readings(created_at, device_id, metric)
SELECT created_at, device_id, 1
  FROM generate_series('2025-09-11 11:00'::timestamptz, '2025-09-11 11:59', '1 minute'::interval) created_at,
       generate_series(1, 4) device_id;

-- Migrate the table to use WAL. We should see the value in the
-- materialization log after the migration
SELECT * FROM materialization_invalidations;
CALL _timescaledb_functions.set_invalidation_method('wal', 'device_readings');
SELECT * FROM materialization_invalidations;

-- Refresh it to check that the refresh works as expected
CALL refresh_continuous_aggregate('device_summary_hourly', NULL, '2025-10-01');
CALL refresh_continuous_aggregate('device_summary_daily', NULL, '2025-10-01');
SELECT * FROM device_summary_hourly;
SELECT * FROM device_summary_daily;

--
-- Test that we can migrate back to use trigger without losing any invalidations.
--

-- Insert new values in the hypertable
INSERT INTO device_readings(created_at, device_id, metric)
SELECT created_at, device_id, 1
  FROM generate_series('2025-09-12 11:00'::timestamptz, '2025-09-12 11:59', '1 minute'::interval) created_at,
       generate_series(1, 4) device_id;

-- Migrate the table to use WAL. We should see the value in the
-- materialization log after the migration
SELECT * FROM materialization_invalidations;
CALL _timescaledb_functions.set_invalidation_method('trigger', 'device_readings');
SELECT * FROM materialization_invalidations;

-- Refresh it to check that the refresh works as expected
CALL refresh_continuous_aggregate('device_summary_hourly', NULL, '2025-10-01');
CALL refresh_continuous_aggregate('device_summary_daily', NULL, '2025-10-01');
SELECT * FROM device_summary_hourly;
SELECT * FROM device_summary_daily;
