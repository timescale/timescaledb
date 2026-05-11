-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test error cases for backfill tracking tenant column configuration.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Setup
CREATE TABLE metrics(
    ts TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    sensor_id UUID,
    temp INTEGER
);

SELECT create_hypertable('metrics', 'ts', chunk_time_interval => interval '6 hours');

CREATE MATERIALIZED VIEW metrics_hourly
WITH (timescaledb.continuous) AS
SELECT device_id, time_bucket('1 hour', ts) AS bucket, avg(temp) AS avg_temp
FROM metrics
GROUP BY device_id, bucket
WITH NO DATA;

\set ON_ERROR_STOP 0
-- Test 1: Tenant column already set
ALTER MATERIALIZED VIEW metrics_hourly SET (timescaledb.tenant_column = 'device_id');

ALTER MATERIALIZED VIEW metrics_hourly SET (timescaledb.tenant_column = 'device_id');

-- Test 2: Column exists on hypertable but is not a GROUP BY column
CREATE MATERIALIZED VIEW metrics_daily
WITH (timescaledb.continuous) AS
SELECT device_id, sensor_id, time_bucket('1 day', ts) AS bucket, avg(temp) AS avg_temp
FROM metrics
GROUP BY device_id, sensor_id, bucket
WITH NO DATA;

ALTER MATERIALIZED VIEW metrics_daily SET (timescaledb.tenant_column = 'temp');

-- Test 3: Sibling cagg with different tenant column
ALTER MATERIALIZED VIEW metrics_daily SET (timescaledb.tenant_column = 'sensor_id');
\set ON_ERROR_STOP 1

-- Cleanup
DROP TABLE metrics CASCADE;
