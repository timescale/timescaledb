-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Regression test: granular refresh with hierarchical continuous
-- aggregates.

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SET timezone TO 'UTC';
SET timescaledb.current_timestamp_mock = '2025-01-10 00:00:00+00';

-- Anchor the start offset safely before the fixed 2020 fixture dates below,
-- computed relative to today so the window keeps covering them regardless
-- of when this test actually runs.
SELECT (CURRENT_DATE - DATE '2019-01-01')::text || ' days' AS granular_refresh_lookback \gset

----------------------------------------------------------------------
-- Base hypertable + granular refresh + base cagg
----------------------------------------------------------------------

CREATE TABLE sensors (time timestamptz NOT NULL, sensor_id integer, temp float8);
SELECT create_hypertable('sensors', 'time', chunk_time_interval => '1 day'::interval);

ALTER TABLE sensors SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 hour'
);

CREATE MATERIALIZED VIEW sensors_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, sensor_id, avg(temp) AS avg_temp
FROM sensors
GROUP BY bucket, sensor_id
WITH NO DATA;

ALTER MATERIALIZED VIEW sensors_hourly SET (timescaledb.enable_granular_refresh = true);

SELECT user_view_name, raw_hypertable_id, granular_refresh_enabled
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'sensors_hourly';

----------------------------------------------------------------------
-- Locate the internal materialization hypertable backing sensors_hourly.
----------------------------------------------------------------------

SELECT format('%I.%I', h.schema_name, h.table_name) AS mat_ht_qualified, h.id AS mat_ht_id
FROM _timescaledb_catalog.continuous_agg ca
JOIN _timescaledb_catalog.hypertable h ON h.id = ca.mat_hypertable_id
WHERE ca.user_view_name = 'sensors_hourly' \gset

----------------------------------------------------------------------
-- Attempt to enable granular refresh directly on the materialization
-- hypertable. NOT blocked: this succeeds like it would on any other
-- hypertable.
----------------------------------------------------------------------

ALTER TABLE :mat_ht_qualified SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = :'granular_refresh_lookback',
    timescaledb.granular_refresh_end_offset = '1 hour'
);

SELECT h.table_name, granular_refresh_column, granular_refresh_start_offset, granular_refresh_end_offset
FROM _timescaledb_catalog.hypertable_cagg_settings s
JOIN _timescaledb_catalog.hypertable h ON h.id = s.hypertable_id
ORDER BY h.id;

----------------------------------------------------------------------
-- Create a hierarchical cagg on top of sensors_hourly and enable granular
-- refresh on it. NOT blocked: the "only one cagg per hypertable" guard
-- keys off raw_hypertable_id, and this cagg's raw_hypertable_id is the
-- materialization hypertable configured above, distinct from
-- sensors_hourly's own raw_hypertable_id (sensors).
----------------------------------------------------------------------

CREATE MATERIALIZED VIEW sensors_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day'::interval, bucket) AS bucket, sensor_id, avg(avg_temp) AS avg_temp
FROM sensors_hourly
GROUP BY 1, sensor_id
WITH NO DATA;

SELECT user_view_name, raw_hypertable_id, parent_mat_hypertable_id, granular_refresh_enabled
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name IN ('sensors_hourly', 'sensors_daily')
ORDER BY user_view_name;

ALTER MATERIALIZED VIEW sensors_daily SET (timescaledb.enable_granular_refresh = true);

SELECT user_view_name, raw_hypertable_id, parent_mat_hypertable_id, granular_refresh_enabled
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name IN ('sensors_hourly', 'sensors_daily')
ORDER BY user_view_name;

----------------------------------------------------------------------
-- Verify that granular refresh is used when refreshing the hierarchical cagg
----------------------------------------------------------------------

INSERT INTO sensors VALUES
  ('2020-01-01 00:00+00', 1, 10),
  ('2020-01-01 00:00+00', 2, 999);
INSERT INTO sensors VALUES ('2025-01-01 00:00+00', 9, 0);

-- Baseline: first-ever materialization of this window is untracked
-- (seqnum 0), so it takes the full-refresh path for both tenants.
CALL refresh_continuous_aggregate('sensors_hourly', '2020-01-01 00:00+00', '2020-01-02 00:00+00');
CALL refresh_continuous_aggregate('sensors_daily', '2020-01-01 00:00+00', '2020-01-02 00:00+00');

SELECT bucket, sensor_id, avg_temp FROM sensors_daily ORDER BY sensor_id;

DROP MATERIALIZED VIEW sensors_daily;
DROP MATERIALIZED VIEW sensors_hourly;
DROP TABLE sensors;
