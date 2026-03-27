-- Test: Hierarchical CAgg Invalidation
-- Tests invalidation propagation through a 4-level hierarchy:
--   sensor_data -> cagg_secondly (L1) -> cagg_minutely (L2) -> cagg_hourly (L3) -> cagg_daily (L4)
--
-- We verify:
-- 1. Initial refresh materializes data correctly
-- 2. New inserts create invalidation entries in the hypertable invalidation log
-- 3. Refreshing L1 processes hypertable invalidations and creates
--    materialization invalidations for L2
-- 4. Refreshing L2 processes its materialization invalidations
-- 5. Final data is correct at all levels

\set ON_ERROR_STOP on
SET datestyle TO 'ISO, YMD';
SET timezone TO 'UTC';

-- ============================================================
-- Setup
-- ============================================================
DROP TABLE IF EXISTS sensor_data CASCADE;

CREATE TABLE sensor_data (
    time   timestamptz NOT NULL,
    device int,
    temp   double precision
);

SELECT create_hypertable('sensor_data', 'time', chunk_time_interval => INTERVAL '1 day');

-- ============================================================
-- L1 CAgg: per-second aggregation on the raw hypertable
-- ============================================================
CREATE MATERIALIZED VIEW cagg_secondly
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 second', time) AS bucket,
       device,
       avg(temp)  AS avg_temp,
       min(temp)  AS min_temp,
       max(temp)  AS max_temp,
       count(*)   AS row_count
FROM sensor_data
GROUP BY 1, 2
WITH NO DATA;

-- ============================================================
-- L2 CAgg: per-minute aggregation on secondly cagg
-- ============================================================
CREATE MATERIALIZED VIEW cagg_minutely
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 minute', bucket) AS bucket,
       device,
       avg(avg_temp) AS avg_temp,
       min(min_temp) AS min_temp,
       max(max_temp) AS max_temp,
       sum(row_count) AS row_count
FROM cagg_secondly
GROUP BY 1, 2
WITH NO DATA;

-- ============================================================
-- L3 CAgg: hourly aggregation on minutely cagg
-- ============================================================
CREATE MATERIALIZED VIEW cagg_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 hour', bucket) AS bucket,
       device,
       avg(avg_temp) AS avg_temp,
       min(min_temp) AS min_temp,
       max(max_temp) AS max_temp,
       sum(row_count) AS row_count
FROM cagg_minutely
GROUP BY 1, 2
WITH NO DATA;

-- ============================================================
-- L4 CAgg: daily aggregation on hourly cagg
-- ============================================================
CREATE MATERIALIZED VIEW cagg_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 day', bucket) AS bucket,
       device,
       avg(avg_temp) AS avg_temp,
       min(min_temp) AS min_temp,
       max(max_temp) AS max_temp,
       sum(row_count) AS row_count
FROM cagg_hourly
GROUP BY 1, 2
WITH NO DATA;

-- ============================================================
-- Helper views to inspect internal state
-- ============================================================

CREATE OR REPLACE VIEW cagg_info AS
SELECT ca.mat_hypertable_id,
       ca.raw_hypertable_id,
       ca.user_view_name,
       format('%I.%I', ht.schema_name, ht.table_name) AS mat_table
FROM _timescaledb_catalog.continuous_agg ca
JOIN _timescaledb_catalog.hypertable ht ON ht.id = ca.mat_hypertable_id
ORDER BY ca.user_view_name;

CREATE OR REPLACE VIEW hyper_invals AS
SELECT hil.hypertable_id,
       _timescaledb_functions.to_timestamp(hil.lowest_modified_value) AS lowest_modified,
       _timescaledb_functions.to_timestamp(hil.greatest_modified_value) AS greatest_modified
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log hil
ORDER BY 1, 2, 3;

CREATE OR REPLACE VIEW mat_invals AS
SELECT mil.materialization_id,
       _timescaledb_functions.to_timestamp(mil.lowest_modified_value) AS lowest_modified,
       _timescaledb_functions.to_timestamp(mil.greatest_modified_value) AS greatest_modified
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log mil
ORDER BY 1, 2, 3;

CREATE OR REPLACE VIEW inval_thresholds AS
SELECT ht.id AS hypertable_id,
       format('%I.%I', ht.schema_name, ht.table_name) AS hypertable,
       _timescaledb_functions.to_timestamp(t.watermark) AS threshold
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold t
JOIN _timescaledb_catalog.hypertable ht ON ht.id = t.hypertable_id
ORDER BY 1;

CREATE OR REPLACE VIEW cagg_watermarks AS
SELECT ca.user_view_name,
       ca.mat_hypertable_id,
       _timescaledb_functions.to_timestamp(w.watermark) AS watermark
FROM _timescaledb_catalog.continuous_aggs_watermark w
JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = w.mat_hypertable_id
ORDER BY ca.user_view_name;

-- Show the CAgg hierarchy
\echo '=== CAgg hierarchy ==='
SELECT * FROM cagg_info;

-- ============================================================
-- Step 1: Insert initial data (3 days, 2 devices)
-- ============================================================
\echo '=== Step 1: Insert initial data (3 days) ==='

INSERT INTO sensor_data (time, device, temp)
SELECT ts, device, 20.0 + (random() * 10)
FROM generate_series('2024-01-01'::timestamptz, '2024-01-03 23:59:59', '10 seconds') ts,
     generate_series(1, 2) device;

SELECT count(*) AS raw_rows FROM sensor_data;

\echo '--- Hypertable invalidation log (before refresh) ---'
SELECT * FROM hyper_invals;

\echo '--- Materialization invalidation log (before refresh) ---'
SELECT * FROM mat_invals;

\echo '--- Invalidation thresholds (before refresh) ---'
SELECT * FROM inval_thresholds;

SELECT count(*) AS secondly_rows FROM cagg_secondly;
SELECT count(*) AS minutely_rows FROM cagg_minutely;
SELECT count(*) AS hourly_rows FROM cagg_hourly;
SELECT count(*) AS daily_rows FROM cagg_daily;

-- ============================================================
-- Step 2: Refresh all levels bottom-up for the full range
-- ============================================================
\echo '=== Step 2: Refresh cagg_secondly (L1) ==='
CALL refresh_continuous_aggregate('cagg_secondly', '2024-01-01', '2024-01-04');
SELECT count(*) AS secondly_rows FROM cagg_secondly;

\echo '=== Step 2: Refresh cagg_minutely (L2) ==='
CALL refresh_continuous_aggregate('cagg_minutely', '2024-01-01', '2024-01-04');
SELECT count(*) AS minutely_rows FROM cagg_minutely;

\echo '=== Step 2: Refresh cagg_hourly (L3) ==='
CALL refresh_continuous_aggregate('cagg_hourly', '2024-01-01', '2024-01-04');
SELECT count(*) AS hourly_rows FROM cagg_hourly;

\echo '=== Step 2: Refresh cagg_daily (L4) ==='
CALL refresh_continuous_aggregate('cagg_daily', '2024-01-01', '2024-01-04');
SELECT count(*) AS daily_rows FROM cagg_daily;
SELECT * FROM cagg_daily ORDER BY bucket, device;

\echo '--- Hypertable invalidation log (after all refreshes) ---'
SELECT * FROM hyper_invals;

\echo '--- Materialization invalidation log (after all refreshes) ---'
SELECT * FROM mat_invals;

\echo '--- Invalidation thresholds (after all refreshes) ---'
SELECT * FROM inval_thresholds;

-- ============================================================
-- Step 4: Insert NEW data for Jan 2 (should create invalidations)
-- ============================================================
\echo '=== Step 4: Insert new data for Jan 2 (temp=99 to make changes visible) ==='

INSERT INTO sensor_data (time, device, temp)
SELECT ts, device, 99.0
FROM generate_series('2024-01-02 00:00:00'::timestamptz, '2024-01-02 23:59:59', '10 seconds') ts,
     generate_series(1, 2) device;

\echo '--- Hypertable invalidation log (after new inserts) ---'
SELECT * FROM hyper_invals;

\echo '--- Materialization invalidation log (after new inserts, should be unchanged) ---'
SELECT * FROM mat_invals;

-- All CAggs should still show OLD data (materialized_only=true)
\echo '--- cagg_daily still shows old data for Jan 2 ---'
SELECT bucket, device,
       round(avg_temp::numeric, 2) AS avg_temp,
       round(max_temp::numeric, 2) AS max_temp,
       row_count
FROM cagg_daily
WHERE bucket = '2024-01-02'
ORDER BY bucket, device;

-- ============================================================
-- Step 5: Refresh L1 (secondly) only — watch invalidation propagate
-- ============================================================
\echo '=== Step 5: Refresh cagg_secondly for Jan 2 only ==='

CALL refresh_continuous_aggregate('cagg_secondly', '2024-01-02', '2024-01-03');

\echo '--- Hypertable invalidation log (after L1 refresh) ---'
SELECT * FROM hyper_invals;

-- KEY CHECK: L1 refresh should create invalidation for L2 (minutely)
\echo '--- Materialization invalidation log (after L1 refresh) ---'
\echo '    Expect new hyper_inval entry for cagg_minutely mat_hypertable'
SELECT * FROM mat_invals;

-- ============================================================
-- Step 6: Refresh L2 (minutely) — propagates to L3
-- ============================================================
\echo '=== Step 6: Refresh cagg_minutely for Jan 2 ==='

CALL refresh_continuous_aggregate('cagg_minutely', '2024-01-02', '2024-01-03');

\echo '--- Hypertable invalidation log (after L2 refresh) ---'
\echo '    Expect new hyper_inval entry for cagg_hourly mat_hypertable'
SELECT * FROM hyper_invals;

\echo '--- Materialization invalidation log (after L2 refresh) ---'
SELECT * FROM mat_invals;

-- ============================================================
-- Step 7: Refresh L3 (hourly) — propagates to L4
-- ============================================================
\echo '=== Step 7: Refresh cagg_hourly for Jan 2 ==='

CALL refresh_continuous_aggregate('cagg_hourly', '2024-01-02', '2024-01-03');

\echo '--- cagg_hourly after refresh (Jan 2, first 5 rows) ---'
SELECT bucket, device,
       round(avg_temp::numeric, 2) AS avg_temp,
       round(max_temp::numeric, 2) AS max_temp,
       row_count
FROM cagg_hourly
WHERE bucket >= '2024-01-02' AND bucket < '2024-01-03'
ORDER BY bucket, device
LIMIT 5;

\echo '--- Hypertable invalidation log (after L3 refresh) ---'
\echo '    Expect new hyper_inval entry for cagg_daily mat_hypertable'
SELECT * FROM hyper_invals;

\echo '--- Materialization invalidation log (after L3 refresh) ---'
SELECT * FROM mat_invals;

-- L4 should still show OLD data
\echo '--- cagg_daily still shows old data (L4 not yet refreshed) ---'
SELECT bucket, device,
       round(avg_temp::numeric, 2) AS avg_temp,
       round(max_temp::numeric, 2) AS max_temp,
       row_count
FROM cagg_daily
WHERE bucket = '2024-01-02'
ORDER BY bucket, device;

-- ============================================================
-- Step 8: Refresh L4 (daily) — final level
-- ============================================================
\echo '=== Step 8: Refresh cagg_daily for Jan 2 ==='

CALL refresh_continuous_aggregate('cagg_daily', '2024-01-02', '2024-01-03');

\echo '--- cagg_daily after refresh (Jan 2) ---'
\echo '    max_temp should now be 99.00, row_count should increase'
SELECT bucket, device,
       round(avg_temp::numeric, 2) AS avg_temp,
       round(min_temp::numeric, 2) AS min_temp,
       round(max_temp::numeric, 2) AS max_temp,
       row_count
FROM cagg_daily
WHERE bucket = '2024-01-02'
ORDER BY bucket, device;

\echo '--- Materialization invalidation log (after all refreshes) ---'
SELECT * FROM mat_invals;

\echo '--- Final invalidation thresholds ---'
SELECT * FROM inval_thresholds;

-- ============================================================
-- Step 9: Verify data consistency across all levels
-- ============================================================
\echo '=== Step 9: Cross-level consistency check (Jan 2) ==='

-- Daily should match re-aggregation from hourly
SELECT d.bucket, d.device,
       round(d.avg_temp::numeric, 2)  AS daily_avg,
       round(h.reagg_avg::numeric, 2) AS from_hourly_avg,
       round(d.max_temp::numeric, 2)  AS daily_max,
       round(h.reagg_max::numeric, 2) AS from_hourly_max,
       d.row_count                    AS daily_count,
       h.reagg_count                  AS from_hourly_count
FROM cagg_daily d
JOIN (
    SELECT time_bucket('1 day', bucket) AS day, device,
           avg(avg_temp) AS reagg_avg,
           max(max_temp) AS reagg_max,
           sum(row_count) AS reagg_count
    FROM cagg_hourly GROUP BY 1, 2
) h ON h.day = d.bucket AND h.device = d.device
WHERE d.bucket = '2024-01-02'
ORDER BY d.bucket, d.device;

-- Hourly should match re-aggregation from minutely
\echo '--- Hourly vs minutely consistency (Jan 2, first 5) ---'
SELECT h.bucket, h.device,
       round(h.avg_temp::numeric, 2)  AS hourly_avg,
       round(m.reagg_avg::numeric, 2) AS from_minutely_avg,
       h.row_count                    AS hourly_count,
       m.reagg_count                  AS from_minutely_count
FROM cagg_hourly h
JOIN (
    SELECT time_bucket('1 hour', bucket) AS hour, device,
           avg(avg_temp) AS reagg_avg,
           sum(row_count) AS reagg_count
    FROM cagg_minutely GROUP BY 1, 2
) m ON m.hour = h.bucket AND m.device = h.device
WHERE h.bucket >= '2024-01-02' AND h.bucket < '2024-01-03'
ORDER BY h.bucket, h.device
LIMIT 5;

-- Minutely should match re-aggregation from secondly
\echo '--- Minutely vs secondly consistency (Jan 2, first 5) ---'
SELECT m.bucket, m.device,
       round(m.avg_temp::numeric, 2)  AS minutely_avg,
       round(s.reagg_avg::numeric, 2) AS from_secondly_avg,
       m.row_count                    AS minutely_count,
       s.reagg_count                  AS from_secondly_count
FROM cagg_minutely m
JOIN (
    SELECT time_bucket('1 minute', bucket) AS minute, device,
           avg(avg_temp) AS reagg_avg,
           sum(row_count) AS reagg_count
    FROM cagg_secondly GROUP BY 1, 2
) s ON s.minute = m.bucket AND s.device = m.device
WHERE m.bucket >= '2024-01-02' AND m.bucket < '2024-01-03'
ORDER BY m.bucket, m.device
LIMIT 5;

-- ============================================================
-- Cleanup
-- ============================================================
\echo '=== Done! ==='
\echo 'To clean up: DROP TABLE sensor_data CASCADE;'
