-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Tests for continuous aggregate invalidation with variable-sized buckets

\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_PERM_USER;
SET datestyle TO 'ISO, YMD';
SET timezone TO 'UTC';

CREATE VIEW hyper_inval_log AS
SELECT ht.schema_name || '.' || ht.table_name AS hypertable,
       _timescaledb_functions.to_timestamp(lowest_modified_value) AS inval_start,
       _timescaledb_functions.to_timestamp(greatest_modified_value) AS inval_end
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log hil
JOIN _timescaledb_catalog.hypertable ht ON ht.id = hil.hypertable_id
ORDER BY 1, 2, 3;

CREATE VIEW cagg_inval_log AS
SELECT ca.user_view_name AS cagg_name,
       _timescaledb_functions.to_timestamp(mil.lowest_modified_value) AS inval_start,
       _timescaledb_functions.to_timestamp(mil.greatest_modified_value) AS inval_end
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log mil
JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = mil.materialization_id
ORDER BY 1, 2, 3;

-----------------------------------------------------------------------
-- SECTION 1: Monthly buckets with varying month lengths
-- Tests that invalidations are correctly processed for variable-width
-- buckets.
-----------------------------------------------------------------------

CREATE TABLE monthly_data (
    time TIMESTAMPTZ NOT NULL,
    device INT,
    value FLOAT
);
SELECT create_hypertable('monthly_data', 'time', chunk_time_interval => INTERVAL '1 month');

-- Create a 1-month bucket cagg
CREATE MATERIALIZED VIEW cagg_monthly
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 month'::interval, time) AS bucket,
       device,
       count(*) AS cnt
FROM monthly_data
GROUP BY 1, 2
WITH NO DATA;

-- Insert data spanning 12 months of 2024 (leap year)
INSERT INTO monthly_data
SELECT ts, 1, extract(epoch FROM ts)::int % 100
FROM generate_series('2024-01-01 00:00:00'::timestamptz,
                     '2024-12-31 23:59:59'::timestamptz,
                     '1 day'::interval) ts;

CALL refresh_continuous_aggregate('cagg_monthly', '2024-01-01 00:00:00', '2025-01-01 00:00:00');

-- Verify data is materialized
SELECT bucket, cnt FROM cagg_monthly ORDER BY bucket;

-- No invalidations should remain after full refresh
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_monthly';

-----------------------------------------------------------------------
-- Test 1a: Invalidation in February (28/29 day month) of a leap year
-- February 2024 has 29 days.
-----------------------------------------------------------------------

INSERT INTO monthly_data VALUES ('2024-02-15 12:00:00', 1, 999.0);

SELECT * FROM hyper_inval_log;

-- Refresh only February
CALL refresh_continuous_aggregate('cagg_monthly', '2024-02-01 00:00:00', '2024-03-01 00:00:00');
SELECT bucket, cnt FROM cagg_monthly WHERE bucket = '2024-02-01';

-- No invalidation should remain for February
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_monthly';

-----------------------------------------------------------------------
-- Test 1b: Invalidation at the exact boundary between Feb 29 and Mar 1
-----------------------------------------------------------------------

-- Insert at the very last moment of Feb 29
INSERT INTO monthly_data VALUES ('2024-02-29 23:59:59.999999', 1, 888.0);
-- Insert at the very first moment of Mar 1
INSERT INTO monthly_data VALUES ('2024-03-01 00:00:00', 1, 777.0);

SELECT * FROM hyper_inval_log;

-- Refresh February only
CALL refresh_continuous_aggregate('cagg_monthly', '2024-02-01 00:00:00', '2024-03-01 00:00:00');

-- The remaining invalidation should only cover March
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_monthly';

-- Now refresh March
CALL refresh_continuous_aggregate('cagg_monthly', '2024-03-01 00:00:00', '2024-04-01 00:00:00');

-- No invalidations should remain
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_monthly';

-----------------------------------------------------------------------
-- Test 1c: Invalidation spanning multiple months of different lengths
-----------------------------------------------------------------------

-- Insert one value in each month
INSERT INTO monthly_data VALUES ('2024-02-29 23:59:59', 1, 100.0);  -- 29-day
INSERT INTO monthly_data VALUES ('2024-03-31 12:00:00', 1, 200.0);  -- 31-day
INSERT INTO monthly_data VALUES ('2024-04-30 23:59:59', 1, 300.0);  -- 30-day

-- Refresh with a window that partially covers all three months.
CALL refresh_continuous_aggregate('cagg_monthly', '2024-02-15 00:00:00', '2024-04-15 00:00:00');
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_monthly';

-- Refresh the whole window to clear all invalidations
CALL refresh_continuous_aggregate('cagg_monthly', '2024-02-01 00:00:00', '2024-05-01 00:00:00');
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_monthly';

-----------------------------------------------------------------------
-- Test 1d: Non-leap year February (28 days)
-----------------------------------------------------------------------

INSERT INTO monthly_data
SELECT ts, 2, 50.0
FROM generate_series('2025-02-01 00:00:00'::timestamptz,
                     '2025-02-28 23:59:59'::timestamptz,
                     '1 day'::interval) ts;

CALL refresh_continuous_aggregate('cagg_monthly', '2025-02-01 00:00:00', '2025-03-01 00:00:00');

-- Verify Feb 2025 bucket has correct number of days
SELECT bucket, cnt FROM cagg_monthly
WHERE device = 2 AND bucket = '2025-02-01 00:00:00';

-- Insert at Feb 28 boundary
INSERT INTO monthly_data VALUES ('2025-02-28 23:59:59.999999', 2, 999.0);
CALL refresh_continuous_aggregate('cagg_monthly', '2025-02-01 00:00:00', '2025-03-01 00:00:00');
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_monthly';

-----------------------------------------------------------------------
-- SECTION 2: Yearly buckets with leap year crossing
-- Tests year-length variability (365 vs 366 days) and the
-- 30-day x 12 = 360-day approximation in bucket_width.
-----------------------------------------------------------------------

CREATE TABLE yearly_data (
    time TIMESTAMPTZ NOT NULL,
    value FLOAT
);
SELECT create_hypertable('yearly_data', 'time', chunk_time_interval => INTERVAL '1 year');

CREATE MATERIALIZED VIEW cagg_yearly
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 year'::interval, time) AS bucket,
       count(*) AS cnt
FROM yearly_data
GROUP BY 1
WITH NO DATA;

INSERT INTO yearly_data
SELECT ts, extract(epoch FROM ts)::int % 1000
FROM generate_series('2024-01-01 00:00:00'::timestamptz,
                     '2025-12-31 00:00:00'::timestamptz,
                     '1 day'::interval) ts;

-- Verify each year bucket has the right number of rows
CALL refresh_continuous_aggregate('cagg_yearly', '2024-01-01 00:00:00', '2026-01-01 00:00:00');
SELECT bucket, cnt FROM cagg_yearly ORDER BY bucket;

-----------------------------------------------------------------------
-- Test 2a: Invalidation crossing year boundary
-----------------------------------------------------------------------

INSERT INTO yearly_data VALUES ('2023-12-31 23:59:59.999999', 1111.0);
INSERT INTO yearly_data VALUES ('2024-01-01 00:00:00', 2222.0);

-- Check that both years are invalidated
SELECT * FROM hyper_inval_log;

-- Refresh only 2023 - should leave 2024 invalidation in the log
CALL refresh_continuous_aggregate('cagg_yearly', '2023-01-01 00:00:00', '2024-01-01 00:00:00');
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_yearly';

CALL refresh_continuous_aggregate('cagg_yearly', '2024-01-01 00:00:00', '2025-01-01 00:00:00');
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_yearly';

-----------------------------------------------------------------------
-- SECTION 3: DST transitions with timezone-aware monthly buckets
-- Tests that bucket boundaries are correct during spring-forward
-- and fall-back DST changes.
-----------------------------------------------------------------------

SET timezone TO 'Europe/Berlin';

CREATE TABLE dst_data (
    time TIMESTAMPTZ NOT NULL,
    value FLOAT
);
SELECT create_hypertable('dst_data', 'time', chunk_time_interval => INTERVAL '1 month');

-- Daily bucket with Europe/Berlin timezone (DST transitions in March and October)
CREATE MATERIALIZED VIEW cagg_dst_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 day'::interval, time, 'Europe/Berlin') AS bucket,
       count(*) AS cnt
FROM dst_data
GROUP BY 1
WITH NO DATA;

-- Insert data around March 2025 DST spring-forward (Mar 30, 2025 at 2:00 AM Europe/Berlin)
INSERT INTO dst_data
SELECT ts, 1.0
FROM generate_series('2025-03-30 00:00:00'::timestamptz,
                     '2025-03-31 23:59:59.999999'::timestamptz,
                     '1 hour'::interval) ts;

CALL refresh_continuous_aggregate('cagg_dst_daily', '2025-03-01 00:00:00', '2025-05-01 00:00:00');
-- March 30 should have 23 hours
SELECT bucket, cnt FROM cagg_dst_daily
ORDER BY bucket;

-----------------------------------------------------------------------
-- Test 3a: Fall-back DST transition (October 2025)
-- Oct 26, 2025 at 3:00 AM Europe/Berlin becomes 2:00 AM (repeated hour)
-----------------------------------------------------------------------

INSERT INTO dst_data
SELECT ts, 2.0
FROM generate_series('2025-10-26 00:00:00'::timestamptz,
                     '2025-10-27 23:59:59.999999'::timestamptz,
                     '1 hour'::interval) ts;

-- Wide window to cover all DST-shifted buckets
CALL refresh_continuous_aggregate('cagg_dst_daily', '2025-10-01 00:00:00', '2026-12-01 00:00:00');

-- October bucket should have extra hour (25-hour day on Oct 26)
SELECT bucket, cnt FROM cagg_dst_daily
WHERE bucket >= '2025-10-01 00:00:00' AND bucket < '2026-01-01 00:00:00'
ORDER BY bucket;

-- Insert near the fall-back boundary
INSERT INTO dst_data VALUES ('2025-10-26 01:00:00', 888.0);  -- 2:00 AM Europe/Berlin (after fall-back)
INSERT INTO dst_data VALUES ('2025-10-26 00:30:00', 777.0);  -- 2:30 AM Europe/Berlin (before fall-back)

CALL refresh_continuous_aggregate('cagg_dst_daily', '2025-09-01 00:00:00', '2026-02-01 00:00:00');
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_dst_daily';

SET timezone TO 'UTC';

-----------------------------------------------------------------------
-- SECTION 4: Two-month buckets
-- Tests 2-month intervals where pairs of months have different totals:
-- Jan+Feb: 59-60 days, Mar+Apr: 61, May+Jun: 61, Jul+Aug: 62,
-- Sep+Oct: 61, Nov+Dec: 61
-----------------------------------------------------------------------

CREATE TABLE bimonthly_data (
    time TIMESTAMPTZ NOT NULL,
    value INT
);
SELECT create_hypertable('bimonthly_data', 'time', chunk_time_interval => INTERVAL '1 month');

CREATE MATERIALIZED VIEW cagg_bimonthly
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('2 months'::interval, time) AS bucket,
       count(*) AS cnt
FROM bimonthly_data
GROUP BY 1
WITH NO DATA;

INSERT INTO bimonthly_data
SELECT ts, 1
FROM generate_series('2025-01-01 00:00:00'::timestamptz,
                     '2025-12-31 00:00:00'::timestamptz,
                     '1 day'::interval) ts;

CALL refresh_continuous_aggregate('cagg_bimonthly', '2025-01-01 00:00:00', '2025-12-31 00:00:00');
SELECT bucket, cnt FROM cagg_bimonthly ORDER BY bucket;

-----------------------------------------------------------------------
-- Test 4a: Invalidation at the boundary between 2-month buckets
-- (Feb 29 / Mar 1 boundary in leap year, also the JanFeb/MarApr bucket boundary)
-----------------------------------------------------------------------

INSERT INTO bimonthly_data VALUES ('2024-02-29 23:59:59.999999', 999);
INSERT INTO bimonthly_data VALUES ('2024-03-01 00:00:00', 888);

-- Refresh only the Jan-Feb bucket
CALL refresh_continuous_aggregate('cagg_bimonthly', '2024-01-01 00:00:00', '2024-03-01 00:00:00');

-- Mar-Apr invalidation should remain
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_bimonthly';

-- Refresh Mar-Apr
CALL refresh_continuous_aggregate('cagg_bimonthly', '2024-03-01 00:00:00', '2024-05-01 00:00:00');
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_bimonthly';


-----------------------------------------------------------------------
-- SECTION 5: Variable-width buckets with offset
-- Tests that invalidations are correctly processed when variable-width
-- buckets are shifted by an offset.
-----------------------------------------------------------------------

CREATE TABLE offset_data (
    time TIMESTAMPTZ NOT NULL,
    device INT,
    value FLOAT
);
SELECT create_hypertable('offset_data', 'time', chunk_time_interval => INTERVAL '1 month');

-- Create a 1-month bucket cagg with a 15-day offset
CREATE MATERIALIZED VIEW cagg_month_offset
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 month'::interval, time, "offset" := INTERVAL '15 days') AS bucket,
       device,
       count(*) AS cnt
FROM offset_data
GROUP BY 1, 2
WITH NO DATA;

-- Insert data spanning 12 months of 2024 (leap year)
INSERT INTO offset_data
SELECT ts, 1, extract(epoch FROM ts)::int % 100
FROM generate_series('2024-01-01 00:00:00'::timestamptz,
                     '2024-12-31 23:59:59'::timestamptz,
                     '1 day'::interval) ts;

CALL refresh_continuous_aggregate('cagg_month_offset', '2024-01-01 00:00:00', '2025-01-01 00:00:00');

-- Buckets should be shifted to the 16th of each month:
SELECT bucket, cnt FROM cagg_month_offset ORDER BY bucket;

-- No invalidations should remain for the refreshed range
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_month_offset';

-----------------------------------------------------------------------
-- Test 5a: Invalidation at an offset bucket boundary (Feb 16)
-----------------------------------------------------------------------

-- Insert just before the bucket boundary
INSERT INTO offset_data VALUES ('2024-02-15 23:59:59.999999', 1, 888.0);
-- Insert at the bucket boundary
INSERT INTO offset_data VALUES ('2024-02-16 00:00:00', 1, 777.0);

SELECT * FROM hyper_inval_log;

CALL refresh_continuous_aggregate('cagg_month_offset', '2024-01-01 00:00:00', '2024-04-01 00:00:00');

SELECT bucket, cnt FROM cagg_month_offset
WHERE bucket >= '2024-01-16 00:00:00' AND bucket <= '2024-02-16 00:00:00'
ORDER BY bucket;

SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_month_offset';

-----------------------------------------------------------------------
-- Test 5b: Partial refresh leaves correct invalidation with offset
-----------------------------------------------------------------------

-- Insert data across two offset bucket boundaries
INSERT INTO offset_data VALUES ('2024-05-15 12:00:00', 1, 100.0);  -- in Apr 16 - May 16 bucket
INSERT INTO offset_data VALUES ('2024-06-20 12:00:00', 1, 200.0);  -- in Jun 16 - Jul 16 bucket

-- Refresh only May
CALL refresh_continuous_aggregate('cagg_month_offset', '2024-04-01 00:00:00', '2024-06-01 00:00:00');

-- Jun 16 - Jul 16 invalidation should remain
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_month_offset';

CALL refresh_continuous_aggregate('cagg_month_offset', '2024-06-01 00:00:00', '2024-08-01 00:00:00');
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_month_offset';

-----------------------------------------------------------------------
-- Test 5d: Offset with timezone and variable-width bucket
-----------------------------------------------------------------------

SET timezone TO 'Europe/Berlin';

CREATE TABLE offset_tz_data (
    time TIMESTAMPTZ NOT NULL,
    value FLOAT
);
SELECT create_hypertable('offset_tz_data', 'time', chunk_time_interval => INTERVAL '1 month');

CREATE MATERIALIZED VIEW cagg_offset_tz
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 day'::interval, time, 'Europe/Berlin', "offset" := INTERVAL '2 hour') AS bucket,
       count(*) AS cnt
FROM offset_tz_data
GROUP BY 1
WITH NO DATA;

-- Insert data around the DST spring-forward (Mar 30, 2025 at 2:00 AM Europe/Berlin)
INSERT INTO offset_tz_data
SELECT ts, 1.0
FROM generate_series('2025-03-28 00:00:00'::timestamptz,
                     '2025-04-01 23:59:59'::timestamptz,
                     '1 hour'::interval) ts;

CALL refresh_continuous_aggregate('cagg_offset_tz', '2025-03-01 00:00:00', '2025-05-01 00:00:00');

SELECT bucket, cnt FROM cagg_offset_tz ORDER BY bucket;

INSERT INTO offset_tz_data VALUES ('2025-03-30 01:59:59.999999', 888.0);
INSERT INTO offset_tz_data VALUES ('2025-03-30 02:00:00', 777.0);

CALL refresh_continuous_aggregate('cagg_offset_tz', '2025-03-01 00:00:00', '2025-06-01 00:00:00');

SELECT bucket, cnt FROM cagg_offset_tz ORDER BY bucket;
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_offset_tz';

SET timezone TO 'UTC';

-----------------------------------------------------------------------
-- SECTION 6: Variable-width buckets with custom origin
-- Tests that invalidations are correctly processed when variable-width
-- buckets use a custom origin.
-----------------------------------------------------------------------

CREATE TABLE origin_data (
    time TIMESTAMPTZ NOT NULL,
    device INT,
    value FLOAT
);
SELECT create_hypertable('origin_data', 'time', chunk_time_interval => INTERVAL '1 month');

-- 2-month buckets with origin in Feb: shifts pairs from Jan+Feb/Mar+Apr to Feb+Mar/Apr+May
CREATE MATERIALIZED VIEW cagg_bimonth_origin
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('2 months'::interval, time, '2025-02-01 00:00:00+00'::timestamptz) AS bucket,
       device,
       count(*) AS cnt
FROM origin_data
GROUP BY 1, 2
WITH NO DATA;

-- Insert data spanning 12 months of 2024 (leap year)
INSERT INTO origin_data
SELECT ts, 1, extract(epoch FROM ts)::int % 100
FROM generate_series('2025-01-01 00:00:00'::timestamptz,
                     '2025-12-31 23:59:59'::timestamptz,
                     '1 day'::interval) ts;

CALL refresh_continuous_aggregate('cagg_bimonth_origin', '2025-01-01 00:00:00', '2026-01-01 00:00:00');

SELECT bucket, cnt FROM cagg_bimonth_origin ORDER BY bucket;

SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_bimonth_origin';

-----------------------------------------------------------------------
-- Test 6a: Invalidation at the origin-shifted bucket boundary (Apr 1)
-----------------------------------------------------------------------

-- Insert at the shifted boundary
INSERT INTO origin_data VALUES ('2025-03-31 23:59:59.999999', 1, 888.0);
INSERT INTO origin_data VALUES ('2025-04-01 00:00:00', 1, 777.0);

SELECT * FROM hyper_inval_log;

CALL refresh_continuous_aggregate('cagg_bimonth_origin', '2025-02-01 00:00:00', '2025-06-01 00:00:00');

-- Verify Feb+Mar and Apr+May buckets are updated
SELECT bucket, cnt FROM cagg_bimonth_origin
WHERE bucket IN ('2025-02-01 00:00:00', '2025-04-01 00:00:00')
ORDER BY bucket;

SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_bimonth_origin';

-----------------------------------------------------------------------
-- Test 6b: Partial refresh leaves correct invalidation with origin
-----------------------------------------------------------------------

-- Insert across two origin-shifted bucket boundaries
INSERT INTO origin_data VALUES ('2025-05-20 12:00:00', 1, 100.0);  -- in Apr+May bucket
INSERT INTO origin_data VALUES ('2025-07-10 12:00:00', 1, 200.0);  -- in Jun+Jul bucket

-- Refresh only the Apr-May window
CALL refresh_continuous_aggregate('cagg_bimonth_origin', '2025-04-01 00:00:00', '2025-06-01 00:00:00');

-- Jun+Jul invalidation should remain
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_bimonth_origin';

CALL refresh_continuous_aggregate('cagg_bimonth_origin', '2025-06-01 00:00:00', '2025-09-01 00:00:00');
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_bimonth_origin';

-----------------------------------------------------------------------
-- Test 6d: Origin with timezone and variable-width bucket
-- 2-month bucket with origin in Feb and Europe/Berlin timezone,
-- covering the DST spring-forward transition.
-----------------------------------------------------------------------

SET timezone TO 'Europe/Berlin';

CREATE TABLE origin_tz_data (
    time TIMESTAMPTZ NOT NULL,
    value FLOAT
);
SELECT create_hypertable('origin_tz_data', 'time', chunk_time_interval => INTERVAL '1 month');

CREATE MATERIALIZED VIEW cagg_origin_tz
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 day'::interval, time, 'Europe/Berlin',
                   origin := '2025-02-01 05:00:00 Europe/Berlin'::timestamptz) AS bucket,
       count(*) AS cnt
FROM origin_tz_data
GROUP BY 1
WITH NO DATA;

-- DST transition on Mar 30 in Europe/Berlin
INSERT INTO origin_tz_data
SELECT ts, 1.0
FROM generate_series('2025-03-29 00:00:00'::timestamptz,
                     '2025-03-31 23:59:59'::timestamptz,
                     '1 hour'::interval) ts;

CALL refresh_continuous_aggregate('cagg_origin_tz', '2025-02-01 00:00:00', '2025-04-01 00:00:00');

SELECT bucket, cnt FROM cagg_origin_tz ORDER BY bucket;

SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_origin_tz';

SET timezone TO 'UTC';

-----------------------------------------------------------------------
-- SECTION 8: Hierarchical continuous aggregates with variable-width buckets
-- Tests invalidation propagation through a two-level hierarchy:
-- Level 1: 1-month buckets on raw hypertable
-- Level 2: 3-month (quarterly) buckets on Level 1
-----------------------------------------------------------------------

CREATE TABLE hier_data (
    time TIMESTAMPTZ NOT NULL,
    value FLOAT
);
SELECT create_hypertable('hier_data', 'time', chunk_time_interval => INTERVAL '1 month');

CREATE MATERIALIZED VIEW cagg_hier_monthly
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('1 month'::interval, time) AS bucket,
       count(*) AS cnt
FROM hier_data
GROUP BY 1
WITH NO DATA;

CREATE MATERIALIZED VIEW cagg_hier_quarterly
WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS
SELECT time_bucket('3 months'::interval, bucket) AS bucket,
       sum(cnt) AS cnt
FROM cagg_hier_monthly
GROUP BY 1
WITH NO DATA;

INSERT INTO hier_data
SELECT ts, 1.0
FROM generate_series('2025-01-01 00:00:00'::timestamptz,
                     '2025-12-31 23:59:59'::timestamptz,
                     '1 day'::interval) ts;

CALL refresh_continuous_aggregate('cagg_hier_monthly', '2025-01-01 00:00:00', '2026-01-01 00:00:00');
CALL refresh_continuous_aggregate('cagg_hier_quarterly', '2025-01-01 00:00:00', '2026-01-01 00:00:00');

SELECT bucket, cnt FROM cagg_hier_monthly ORDER BY bucket;
SELECT bucket, cnt FROM cagg_hier_quarterly ORDER BY bucket;

SELECT * FROM cagg_inval_log WHERE cagg_name IN ('cagg_hier_monthly', 'cagg_hier_quarterly') ORDER BY 1, 2;

-----------------------------------------------------------------------
-- Test 8a: Insert into base, refresh Level 1 only, then Level 2
-- Verifies that refreshing Level 1 creates invalidation in Level 2
-- and that Level 2 refresh picks up the change.
-----------------------------------------------------------------------

INSERT INTO hier_data VALUES ('2025-02-15 12:00:00', 999.0);
CALL refresh_continuous_aggregate('cagg_hier_monthly', '2025-02-01 00:00:00', '2025-03-01 00:00:00');
SELECT bucket, cnt FROM cagg_hier_monthly WHERE bucket = '2025-02-01 00:00:00';

SELECT * FROM hyper_inval_log;
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_hier_quarterly';

-- Refresh Level 2 for Q1
CALL refresh_continuous_aggregate('cagg_hier_quarterly', '2025-01-01 00:00:00', '2025-04-01 00:00:00');
SELECT bucket, cnt FROM cagg_hier_quarterly WHERE bucket = '2025-01-01 00:00:00';
SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_hier_quarterly';

-----------------------------------------------------------------------
-- Test 8b: Invalidation at the quarter boundary (Mar 31 / Apr 1)
-- Mar 31 is in Q1, Apr 1 is in Q2. Refreshing Level 1 modifies both
-- March and April in the mat table, so Level 2 should update both
-- Q1 and Q2.
-----------------------------------------------------------------------

INSERT INTO hier_data VALUES ('2025-03-31 23:59:59.999999', 888.0);
INSERT INTO hier_data VALUES ('2025-04-01 00:00:00', 777.0);

-- Refresh Level 1 for March and April
CALL refresh_continuous_aggregate('cagg_hier_monthly', '2025-03-01 00:00:00', '2025-05-01 00:00:00');

SELECT bucket, cnt FROM cagg_hier_monthly
WHERE bucket IN ('2025-03-01 00:00:00', '2025-04-01 00:00:00')
ORDER BY bucket;

-- Refresh Level 2 with a wide window covering both Q1 and Q2
CALL refresh_continuous_aggregate('cagg_hier_quarterly', '2025-01-01 00:00:00', '2025-07-01 00:00:00');

SELECT bucket, cnt FROM cagg_hier_quarterly
WHERE bucket IN ('2025-01-01 00:00:00', '2025-04-01 00:00:00')
ORDER BY bucket;

SELECT * FROM cagg_inval_log WHERE cagg_name = 'cagg_hier_quarterly';

DROP MATERIALIZED VIEW cagg_hier_quarterly;
DROP TABLE monthly_data CASCADE;
DROP TABLE yearly_data CASCADE;
DROP TABLE dst_data CASCADE;
DROP TABLE bimonthly_data CASCADE;
DROP TABLE offset_data CASCADE;
DROP TABLE offset_tz_data CASCADE;
DROP TABLE origin_data CASCADE;
DROP TABLE origin_tz_data CASCADE;
DROP TABLE hier_data CASCADE;

DROP VIEW hyper_inval_log;
DROP VIEW cagg_inval_log;
