-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Regression test: granular refresh on hierarchical continuous
-- aggregates.

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SET timezone TO 'UTC';

----------------------------------------------------------------------
-- Base hypertable + granular refresh + base cagg
----------------------------------------------------------------------

CREATE TABLE sensors (time timestamptz NOT NULL, sensor_id integer, temp float8);
SELECT create_hypertable('sensors', 'time', chunk_time_interval => '1 day'::interval);

ALTER TABLE sensors SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = '1 day',
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
-- hypertable. BLOCKED: ALTER TABLE is blocked if run on a
-- continuous aggregate's materialization hypertable.
----------------------------------------------------------------------

\set ON_ERROR_STOP 0
ALTER TABLE :mat_ht_qualified SET (
    timescaledb.granular_refresh_column = 'sensor_id',
    timescaledb.granular_refresh_start_offset = '1 day',
    timescaledb.granular_refresh_end_offset = '1 hour'
);
\set ON_ERROR_STOP 1

----------------------------------------------------------------------
-- Attempt to enable granular refresh on a hierarchical CAgg
-- BLOCKED: Errors since it is not configured on the hypertable
----------------------------------------------------------------------

CREATE MATERIALIZED VIEW sensors_daily
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', bucket) AS bucket, sensor_id, count(avg_temp) AS avg_temp
FROM sensors_hourly
GROUP BY 1, 2
WITH NO DATA;

\set ON_ERROR_STOP 0
ALTER MATERIALIZED VIEW sensors_daily SET (timescaledb.enable_granular_refresh = true);
\set ON_ERROR_STOP 1

DROP MATERIALIZED VIEW sensors_daily;
DROP MATERIALIZED VIEW sensors_hourly;
DROP TABLE sensors;
