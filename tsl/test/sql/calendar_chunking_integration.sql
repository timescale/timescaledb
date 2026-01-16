-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--
-- Test calendar-based chunking with continuous aggregates, compression,
-- and retention policies.
--

SET timescaledb.enable_calendar_chunking = true;
SET timezone TO 'Europe/Stockholm';

---------------------------------------------------------------
-- SETUP: Create hypertables with calendar-based chunking
---------------------------------------------------------------

-- Monthly chunks with calendar alignment
CREATE TABLE metrics_monthly(
    time timestamptz NOT NULL,
    device_id int NOT NULL,
    value float NOT NULL
);

SELECT create_hypertable('metrics_monthly', 'time', chunk_time_interval => INTERVAL '1 month');

-- Weekly chunks with calendar alignment
CREATE TABLE metrics_weekly(
    time timestamptz NOT NULL,
    device_id int NOT NULL,
    value float NOT NULL
);

SELECT create_hypertable('metrics_weekly', 'time', chunk_time_interval => INTERVAL '7 days');

-- Daily chunks with calendar alignment
CREATE TABLE metrics_daily(
    time timestamptz NOT NULL,
    device_id int NOT NULL,
    value float NOT NULL
);

SELECT create_hypertable('metrics_daily', 'time', chunk_time_interval => INTERVAL '1 day');

-- Create table with custom origin (fiscal year starting April 1)
CREATE TABLE metrics_fiscal(
    time timestamptz NOT NULL,
    value float NOT NULL
);

SELECT create_hypertable('metrics_fiscal',
    by_range('time', INTERVAL '1 month', partition_origin => '2024-04-01'::timestamptz));

-- Create a hypertable for CAgg tests
CREATE TABLE cagg_test_monthly(
    time timestamptz NOT NULL,
    device_id int NOT NULL,
    value float NOT NULL
);

SELECT create_hypertable('cagg_test_monthly', 'time', chunk_time_interval => INTERVAL '1 month');

---------------------------------------------------------------
-- INSERT DATA spanning multiple chunks
---------------------------------------------------------------

-- Use setseed for deterministic random values
SELECT setseed(0.1);

-- Insert data for monthly table (spanning 3 months)
INSERT INTO metrics_monthly
SELECT ts, device_id, random() * 100
FROM generate_series('2024-01-15'::timestamptz, '2024-03-15'::timestamptz, '1 day'::interval) ts
CROSS JOIN generate_series(1, 3) device_id;

-- Insert data for weekly table (spanning 4 weeks)
INSERT INTO metrics_weekly
SELECT ts, device_id, random() * 100
FROM generate_series('2024-01-01'::timestamptz, '2024-01-28'::timestamptz, '6 hours'::interval) ts
CROSS JOIN generate_series(1, 3) device_id;

-- Insert data for daily table (spanning 10 days)
INSERT INTO metrics_daily
SELECT ts, device_id, random() * 100
FROM generate_series('2024-01-01'::timestamptz, '2024-01-10'::timestamptz, '1 hour'::interval) ts
CROSS JOIN generate_series(1, 3) device_id;

-- Insert data for fiscal table (reset seed for deterministic values)
SELECT setseed(0.2);

INSERT INTO metrics_fiscal
SELECT ts, random() * 100
FROM generate_series('2024-03-15'::timestamptz, '2024-05-15'::timestamptz, '1 day'::interval) ts;

-- Insert data for CAgg test table
INSERT INTO cagg_test_monthly
SELECT ts, device_id, (EXTRACT(epoch FROM ts) % 100)::float
FROM generate_series('2024-01-15'::timestamptz, '2024-04-15'::timestamptz, '1 hour'::interval) ts
CROSS JOIN generate_series(1, 3) device_id;

---------------------------------------------------------------
-- CONTINUOUS AGGREGATES on calendar-chunked hypertables
--
-- Test that continuous aggregates work correctly on calendar-chunked
-- hypertables. The materialization hypertable should also use
-- calendar-based chunking, inheriting the interval type and origin
-- from the source hypertable.
---------------------------------------------------------------

-- Test 1: CAgg with month bucket (variable-width) on calendar-chunked hypertable
CREATE MATERIALIZED VIEW cagg_monthly
WITH (timescaledb.continuous, timescaledb.materialized_only = true)
AS SELECT
    time_bucket('1 month', time, timezone => 'Europe/Stockholm') AS bucket,
    device_id,
    count(*) as cnt
FROM cagg_test_monthly
GROUP BY 1, 2
WITH NO DATA;

-- Test 2: CAgg with day bucket (fixed-width) on calendar-chunked hypertable
CREATE MATERIALIZED VIEW cagg_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = true)
AS SELECT
    time_bucket('1 day', time, timezone => 'Europe/Stockholm') AS bucket,
    device_id,
    count(*) as cnt
FROM cagg_test_monthly
GROUP BY 1, 2
WITH NO DATA;

-- Test 3: CAgg with hour bucket (fixed-width) on calendar-chunked hypertable
CREATE MATERIALIZED VIEW cagg_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = true)
AS SELECT
    time_bucket('1 hour', time, timezone => 'Europe/Stockholm') AS bucket,
    device_id,
    count(*) as cnt
FROM cagg_test_monthly
GROUP BY 1, 2
WITH NO DATA;

-- Test 4: CAgg on metrics_fiscal (calendar-chunked with custom origin)
CREATE MATERIALIZED VIEW cagg_fiscal
WITH (timescaledb.continuous, timescaledb.materialized_only = true)
AS SELECT
    time_bucket('1 day', time, timezone => 'Europe/Stockholm') AS bucket,
    count(*) as cnt
FROM metrics_fiscal
GROUP BY 1
WITH NO DATA;

-- Verify CAggs were created successfully
SELECT view_name, materialized_only, compression_enabled
FROM timescaledb_information.continuous_aggregates
WHERE view_name LIKE 'cagg_%'
ORDER BY view_name;

-- Refresh the CAggs and verify data
CALL refresh_continuous_aggregate('cagg_monthly', NULL, NULL);
CALL refresh_continuous_aggregate('cagg_daily', NULL, NULL);

SELECT bucket, device_id, cnt
FROM cagg_monthly
ORDER BY bucket, device_id
LIMIT 12;

---------------------------------------------------------------
-- Verify CAgg materialization hypertables use calendar-based chunking
--
-- When a CAgg is created on a calendar-chunked hypertable, the
-- materialization hypertable should also use calendar-based chunking.
-- This is indicated by interval IS NOT NULL in the dimension catalog.
---------------------------------------------------------------

-- Check that materialization hypertables have calendar-based dimensions
-- (interval IS NOT NULL means calendar chunking, interval_length IS NULL)
SELECT
    h.table_name,
    d.column_name,
    d.interval_length IS NULL as is_calendar_chunking,
    d.interval IS NOT NULL as has_interval
FROM _timescaledb_catalog.hypertable h
JOIN _timescaledb_catalog.dimension d ON h.id = d.hypertable_id
WHERE h.table_name LIKE '_materialized_hypertable_%'
ORDER BY h.table_name;

-- Show chunk ranges for cagg_monthly to verify calendar alignment
-- Chunks should align to month boundaries (10 months interval due to MATPARTCOL_INTERVAL_FACTOR)
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = '_materialized_hypertable_6'
ORDER BY range_start;

-- Show chunk ranges for cagg_daily to verify calendar alignment
-- (10 day chunks due to MATPARTCOL_INTERVAL_FACTOR applied to source's 1 month interval)
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = '_materialized_hypertable_7'
ORDER BY range_start;

---------------------------------------------------------------
-- COMPRESSION with calendar-chunked hypertables
---------------------------------------------------------------

-- Enable compression on the tables
ALTER TABLE metrics_monthly SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'time'
);

ALTER TABLE metrics_weekly SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'time'
);

ALTER TABLE metrics_daily SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = 'time'
);

-- Compress specific chunks
SELECT compress_chunk(chunk) FROM show_chunks('metrics_monthly', older_than => '2024-03-01') chunk;
SELECT compress_chunk(chunk) FROM show_chunks('metrics_weekly', older_than => '2024-01-21') chunk;
SELECT compress_chunk(chunk) FROM show_chunks('metrics_daily', older_than => '2024-01-08') chunk;

-- Verify compression status
SELECT chunk_name, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_monthly'
ORDER BY range_start;

SELECT chunk_name, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_weekly'
ORDER BY range_start;

-- Query compressed data to verify it still works
SELECT time_bucket('1 week', time, current_setting('timezone')) as week, count(*), round(avg(value)::numeric, 2) as avg_val
FROM metrics_monthly
GROUP BY 1
ORDER BY 1;

---------------------------------------------------------------
-- VERIFY DATA INTEGRITY after compression
---------------------------------------------------------------

-- Count rows in original table (should include compressed chunks)
SELECT count(*) as total_rows FROM metrics_monthly;

-- Verify aggregation still works correctly
SELECT device_id, count(*), round(avg(value)::numeric, 2) as avg_val
FROM metrics_monthly
GROUP BY device_id
ORDER BY device_id;

---------------------------------------------------------------
-- COMPRESSION POLICY with calendar-chunked hypertables
---------------------------------------------------------------

-- Add compression policy with 1 month threshold
SELECT add_compression_policy('metrics_monthly', INTERVAL '1 month', schedule_interval => INTERVAL '1 year') as monthly_compress_job \gset

-- Decompress all chunks so the policy has work to do
SELECT decompress_chunk(c.chunk_schema || '.' || c.chunk_name)
FROM timescaledb_information.chunks c
WHERE c.hypertable_name = 'metrics_monthly' AND c.is_compressed;

-- Mock time to 2024-03-01 - with 1 month policy, Jan chunk will be compressed, Feb and Mar won't
SET timescaledb.current_timestamp_mock = '2024-03-01 00:00:00+01';

-- Show chunks before running compression policy
SELECT chunk_name, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_monthly'
ORDER BY range_start;

-- Run the compression policy job
CALL run_job(:monthly_compress_job);

-- Verify only Jan chunk was compressed (older than 1 month from mock time)
SELECT chunk_name, is_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_monthly'
ORDER BY range_start;

RESET timescaledb.current_timestamp_mock;

---------------------------------------------------------------
-- RETENTION POLICY with calendar-chunked hypertables
---------------------------------------------------------------

-- Add retention policy with 2 week threshold
SELECT add_retention_policy('metrics_weekly', INTERVAL '2 weeks', schedule_interval => INTERVAL '1 year') as weekly_retention_job \gset

-- Mock time to 2024-02-01 - with 2 week policy, first two chunks will be dropped
SET timescaledb.current_timestamp_mock = '2024-02-01 00:00:00+01';

-- Show chunks before running retention policy (4 chunks: Jan 1-7, 8-14, 15-21, 22-28)
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_weekly'
ORDER BY range_start;

-- Run the retention policy job
CALL run_job(:weekly_retention_job);

-- Verify first two chunks were dropped (older than 2 weeks from mock time)
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'metrics_weekly'
ORDER BY range_start;

RESET timescaledb.current_timestamp_mock;

---------------------------------------------------------------
-- CLEANUP
---------------------------------------------------------------

-- Drop CAggs first (they depend on the tables)
DROP MATERIALIZED VIEW cagg_monthly;
DROP MATERIALIZED VIEW cagg_daily;
DROP MATERIALIZED VIEW cagg_hourly;
DROP MATERIALIZED VIEW cagg_fiscal;
DROP TABLE cagg_test_monthly;

-- Remove policies
SELECT remove_retention_policy('metrics_weekly', if_exists => true);
SELECT remove_compression_policy('metrics_monthly', if_exists => true);

-- Drop objects
DROP TABLE metrics_monthly;
DROP TABLE metrics_weekly;
DROP TABLE metrics_daily;
DROP TABLE metrics_fiscal;

RESET timescaledb.enable_calendar_chunking;
