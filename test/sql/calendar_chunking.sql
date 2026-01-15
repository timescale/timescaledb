-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--
-- Test calendar-based chunking
--
-- Calendar-based chunking aligns chunks with calendar boundaries
-- (e.g., start of day, week, month, year) based on a user-specified origin
-- and the current session timezone.
--

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION calc_range(ts TIMESTAMPTZ, chunk_interval INTERVAL, origin TIMESTAMPTZ DEFAULT NULL, force_general BOOL DEFAULT NULL)
RETURNS TABLE(start_ts TIMESTAMPTZ, end_ts TIMESTAMPTZ) AS :MODULE_PATHNAME, 'ts_dimension_calculate_open_range_calendar' LANGUAGE C;

-- C unit tests for chunk_range.c
CREATE OR REPLACE FUNCTION test_chunk_range()
RETURNS VOID AS :MODULE_PATHNAME, 'ts_test_chunk_range' LANGUAGE C;
SET ROLE :ROLE_DEFAULT_PERM_USER;

\set VERBOSITY terse
SET timescaledb.enable_calendar_chunking = true;

---------------------------------------------------------------
-- CALC_RANGE TESTS
-- Test the calc_range() function with various intervals and timestamps
---------------------------------------------------------------

-- Helper function to verify ranges
CREATE OR REPLACE FUNCTION test_ranges(
    timestamps TIMESTAMPTZ[],
    intervals INTERVAL[]
) RETURNS TABLE(ts TIMESTAMPTZ, inv INTERVAL, start_ts TIMESTAMPTZ, end_ts TIMESTAMPTZ, dur INTERVAL, in_range BOOLEAN) AS $$
    SELECT t.ts, i.inv, r.start_ts, r.end_ts, r.end_ts - r.start_ts, t.ts >= r.start_ts AND t.ts < r.end_ts
    FROM unnest(timestamps) AS t(ts)
    CROSS JOIN unnest(intervals) AS i(inv)
    CROSS JOIN LATERAL calc_range(t.ts, i.inv) r
    ORDER BY t.ts, i.inv;
$$ LANGUAGE SQL;

-- Helper function to verify ranges with custom origin
CREATE OR REPLACE FUNCTION test_ranges_with_origin(
    timestamps TIMESTAMPTZ[],
    intervals INTERVAL[],
    origin TIMESTAMPTZ
) RETURNS TABLE(ts TIMESTAMPTZ, inv INTERVAL, start_ts TIMESTAMPTZ, end_ts TIMESTAMPTZ, dur INTERVAL, in_range BOOLEAN) AS $$
    SELECT t.ts, i.inv, r.start_ts, r.end_ts, r.end_ts - r.start_ts, t.ts >= r.start_ts AND t.ts < r.end_ts
    FROM unnest(timestamps) AS t(ts)
    CROSS JOIN unnest(intervals) AS i(inv)
    CROSS JOIN LATERAL calc_range(t.ts, i.inv, origin) r
    ORDER BY t.ts, i.inv;
$$ LANGUAGE SQL;

-- Helper function to show CHECK constraints with chunk size for time-based hypertables
-- Uses CASE to handle infinite timestamps (older PG versions can't subtract them)
CREATE OR REPLACE FUNCTION show_chunk_constraints(ht_name text)
RETURNS TABLE(chunk text, constraint_def text, chunk_size interval) AS $$
    SELECT c.table_name::text,
           pg_get_constraintdef(con.oid),
           CASE WHEN ch.range_start = '-infinity'::timestamptz
                  OR ch.range_end = 'infinity'::timestamptz
                THEN NULL
                ELSE ch.range_end - ch.range_start
           END
    FROM _timescaledb_catalog.chunk c
    JOIN pg_constraint con ON con.conrelid = format('%I.%I', c.schema_name, c.table_name)::regclass
    JOIN timescaledb_information.chunks ch ON ch.chunk_name = c.table_name AND ch.hypertable_name = ht_name
    WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = ht_name)
      AND con.contype = 'c'
    ORDER BY c.table_name;
$$ LANGUAGE SQL;

-- Helper function to show dimension slices with chunk size for integer hypertables
CREATE OR REPLACE FUNCTION show_int_chunk_slices(ht_name text)
RETURNS TABLE(chunk_name text, range_start bigint, range_end bigint, chunk_size bigint) AS $$
    SELECT c.table_name::text, ds.range_start, ds.range_end,
           ds.range_end - ds.range_start
    FROM _timescaledb_catalog.chunk c
    JOIN _timescaledb_catalog.chunk_constraint cc ON c.id = cc.chunk_id
    JOIN _timescaledb_catalog.dimension_slice ds ON cc.dimension_slice_id = ds.id
    WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = ht_name)
    ORDER BY ds.range_start;
$$ LANGUAGE SQL;

-- Helper function to show CHECK constraints only (for integer hypertables)
CREATE OR REPLACE FUNCTION show_check_constraints(ht_name text)
RETURNS TABLE(chunk text, constraint_def text) AS $$
    SELECT c.table_name::text,
           pg_get_constraintdef(con.oid)
    FROM _timescaledb_catalog.chunk c
    JOIN pg_constraint con ON con.conrelid = format('%I.%I', c.schema_name, c.table_name)::regclass
    WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = ht_name)
      AND con.contype = 'c'
    ORDER BY c.table_name;
$$ LANGUAGE SQL;

-- Basic interval tests in UTC
SET timezone = 'UTC';

SELECT * FROM test_ranges(
    ARRAY[
        '2024-01-15 12:30:45 UTC'::timestamptz,
        '2024-06-15 00:00:00 UTC',
        '2024-06-15 23:59:59.999999 UTC'
    ],
    ARRAY[
        '1 minute'::interval, '5 minutes', '15 minutes', '30 minutes',
        '1 hour', '2 hours', '4 hours', '6 hours', '12 hours',
        '1 day', '1 week', '1 month', '3 months', '6 months', '1 year'
    ]
);

-- Leap year tests
SELECT * FROM test_ranges(
    ARRAY[
        '2024-02-28 12:00:00 UTC'::timestamptz,  -- leap year
        '2024-02-29 00:00:00 UTC',
        '2024-02-29 23:59:59 UTC',
        '2024-03-01 00:00:00 UTC',
        '2025-02-28 12:00:00 UTC',  -- non-leap year
        '2025-02-28 23:59:59 UTC',
        '2025-03-01 00:00:00 UTC'
    ],
    ARRAY['1 day'::interval, '1 month']
);

-- Week alignment (should align to Monday)
SELECT * FROM test_ranges(
    ARRAY[
        '2024-06-10 12:00:00 UTC'::timestamptz,  -- Monday
        '2024-06-11 12:00:00 UTC',  -- Tuesday
        '2024-06-14 12:00:00 UTC',  -- Friday
        '2024-06-16 12:00:00 UTC',  -- Sunday
        '2024-06-17 00:00:00 UTC'   -- Monday (next week)
    ],
    ARRAY['1 week'::interval, '2 weeks']
);

-- Year boundary tests
SELECT * FROM test_ranges(
    ARRAY[
        '2024-12-31 23:59:59 UTC'::timestamptz,
        '2025-01-01 00:00:00 UTC'
    ],
    ARRAY['1 day'::interval, '1 week', '1 month', '1 year']
);

-- Custom origin: days starting at noon
SELECT * FROM test_ranges_with_origin(
    ARRAY[
        '2024-06-15 11:59:59 UTC'::timestamptz,
        '2024-06-15 12:00:00 UTC',
        '2024-06-15 23:59:59 UTC',
        '2024-06-16 00:00:00 UTC',
        '2024-06-16 11:59:59 UTC',
        '2024-06-16 12:00:00 UTC'
    ],
    ARRAY['1 day'::interval],
    '2020-01-01 12:00:00 UTC'
);

-- Custom origin: 15 minutes past midnight
SELECT * FROM test_ranges_with_origin(
    ARRAY[
        '2024-06-15 00:00:00 UTC'::timestamptz,
        '2024-06-15 00:14:59 UTC',
        '2024-06-15 00:15:00 UTC',
        '2024-06-15 12:00:00 UTC',
        '2024-06-16 00:14:59 UTC',
        '2024-06-16 00:15:00 UTC'
    ],
    ARRAY['1 day'::interval],
    '2020-01-01 00:15:00 UTC'
);

-- Custom origin: hourly chunks starting at 30 minutes
SELECT * FROM test_ranges_with_origin(
    ARRAY[
        '2024-06-15 12:00:00 UTC'::timestamptz,
        '2024-06-15 12:29:59 UTC',
        '2024-06-15 12:30:00 UTC',
        '2024-06-15 13:29:59 UTC',
        '2024-06-15 13:30:00 UTC'
    ],
    ARRAY['1 hour'::interval],
    '2020-01-01 00:30:00 UTC'
);

---------------------------------------------------------------
-- DST TRANSITION TESTS
---------------------------------------------------------------

-- America/Los_Angeles DST spring forward (Mar 10)
SET timezone = 'America/Los_Angeles';

SELECT * FROM test_ranges(
    ARRAY[
        '2024-03-09 23:59:59 America/Los_Angeles'::timestamptz,
        '2024-03-10 00:00:00 America/Los_Angeles',
        '2024-03-10 01:59:59 America/Los_Angeles',
        '2024-03-10 03:00:00 America/Los_Angeles',  -- 2 AM doesn't exist
        '2024-03-11 00:00:00 America/Los_Angeles'
    ],
    ARRAY['1 hour'::interval, '1 day', '1 week']
);

-- America/Los_Angeles DST fall back (Nov 3)
SELECT * FROM test_ranges(
    ARRAY[
        '2024-11-02 23:59:59 America/Los_Angeles'::timestamptz,
        '2024-11-03 00:00:00 America/Los_Angeles',
        '2024-11-03 01:30:00 PDT',  -- first 1:30 AM
        '2024-11-03 01:30:00 PST',  -- second 1:30 AM
        '2024-11-03 02:00:00 America/Los_Angeles'
    ],
    ARRAY['1 hour'::interval, '1 day']
);

-- Europe/London DST
SET timezone = 'Europe/London';

SELECT * FROM test_ranges(
    ARRAY[
        '2024-03-31 00:59:59 Europe/London'::timestamptz,  -- before spring forward
        '2024-03-31 02:00:00 Europe/London',  -- after spring forward
        '2024-10-27 01:30:00 BST',  -- before fall back
        '2024-10-27 01:30:00 GMT'   -- after fall back
    ],
    ARRAY['1 hour'::interval, '1 day']
);

-- Use a timezone with 45-minute offset for hypertable tests
SET timezone = 'Pacific/Chatham';

---------------------------------------------------------------
-- HYPERTABLE TESTS: UUID v7
---------------------------------------------------------------

CREATE TABLE uuid_events(
    id uuid PRIMARY KEY,
    device_id int,
    temp float
);

SELECT create_hypertable('uuid_events', 'id', chunk_time_interval => interval '1 day');

-- UUIDs for known timestamps:
-- 2025-01-01 02:00 UTC, 2025-01-01 09:00 UTC
-- 2025-01-02 03:00 UTC, 2025-01-02 04:00 UTC
-- 2025-01-03 05:00 UTC, 2025-01-03 12:00 UTC
INSERT INTO uuid_events VALUES
    ('01942117-de80-7000-8121-f12b2b69dd96', 1, 1.0),
    ('0194214e-cd00-7000-a9a7-63f1416dab45', 2, 2.0),
    ('0194263e-3a80-7000-8f40-82c987b1bc1f', 3, 3.0),
    ('01942675-2900-7000-8db1-a98694b18785', 4, 4.0),
    ('01942bd2-7380-7000-9bc4-5f97443907b8', 5, 5.0),
    ('01942d52-f900-7000-866e-07d6404d53c1', 6, 6.0);

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'uuid_events' ORDER BY range_start;

DROP TABLE uuid_events;

---------------------------------------------------------------
-- HYPERTABLE TESTS: TIMESTAMPTZ
---------------------------------------------------------------

-- Daily chunks
CREATE TABLE tz_daily(time timestamptz NOT NULL, value int);
SELECT create_hypertable('tz_daily', 'time', chunk_time_interval => interval '1 day');
INSERT INTO tz_daily VALUES
    ('2025-01-01 00:00:00 UTC', 1),
    ('2025-01-01 23:59:59.999999 UTC', 2),
    ('2025-01-02 00:00:00 UTC', 3);
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_daily' ORDER BY range_start;
DROP TABLE tz_daily;

-- Weekly chunks
CREATE TABLE tz_weekly(time timestamptz NOT NULL, value int);
SELECT create_hypertable('tz_weekly', 'time', chunk_time_interval => interval '1 week');
INSERT INTO tz_weekly VALUES
    ('2025-01-06 00:00:00 UTC', 1),  -- Monday
    ('2025-01-12 23:59:59 UTC', 2),  -- Sunday
    ('2025-01-13 00:00:00 UTC', 3);  -- Monday (next week)
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_weekly' ORDER BY range_start;
DROP TABLE tz_weekly;

-- Monthly chunks
CREATE TABLE tz_monthly(time timestamptz NOT NULL, value int);
SELECT create_hypertable('tz_monthly', 'time', chunk_time_interval => interval '1 month');
INSERT INTO tz_monthly VALUES
    ('2025-01-15 12:00:00 UTC', 1),
    ('2025-02-15 12:00:00 UTC', 2),
    ('2025-03-15 12:00:00 UTC', 3);
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_monthly' ORDER BY range_start;
DROP TABLE tz_monthly;

-- Yearly chunks
CREATE TABLE tz_yearly(time timestamptz NOT NULL, value int);
SELECT create_hypertable('tz_yearly', 'time', chunk_time_interval => interval '1 year');
INSERT INTO tz_yearly VALUES
    ('2024-06-15 12:00:00 UTC', 1),
    ('2025-06-15 12:00:00 UTC', 2);
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_yearly' ORDER BY range_start;
DROP TABLE tz_yearly;

-- DST spring forward test: 23-hour chunk
-- America/New_York DST 2025: Mar 9 at 2:00 AM clocks spring forward to 3:00 AM
SET timezone = 'America/New_York';

CREATE TABLE tz_dst_spring(time timestamptz NOT NULL, value int);
SELECT create_hypertable('tz_dst_spring', 'time', chunk_time_interval => interval '1 day');

INSERT INTO tz_dst_spring VALUES
    ('2025-03-08 12:00:00', 1),  -- day before DST (24 hours)
    ('2025-03-09 12:00:00', 2),  -- DST day (23 hours - spring forward)
    ('2025-03-10 12:00:00', 3);  -- day after DST (24 hours)

SELECT chunk_name, range_start, range_end,
       round(extract(epoch FROM (range_end - range_start)) / 3600, 1) AS hours
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_dst_spring' ORDER BY range_start;

DROP TABLE tz_dst_spring;

-- DST fall back test: 25-hour chunk
-- America/New_York 2024: Nov 3 at 2:00 AM clocks fall back to 1:00 AM
CREATE TABLE tz_dst_fall(time timestamptz NOT NULL, value int);
SELECT create_hypertable('tz_dst_fall', 'time', chunk_time_interval => interval '1 day');

INSERT INTO tz_dst_fall VALUES
    ('2024-11-02 12:00:00', 1),  -- day before DST (24 hours)
    ('2024-11-03 12:00:00', 2),  -- DST day (25 hours - fall back)
    ('2024-11-04 12:00:00', 3);  -- day after DST (24 hours)

SELECT chunk_name, range_start, range_end,
       round(extract(epoch FROM (range_end - range_start)) / 3600, 1) AS hours
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_dst_fall' ORDER BY range_start;

DROP TABLE tz_dst_fall;

-- Same test in UTC - all chunks should be exactly 24 hours
SET timezone = 'UTC';

CREATE TABLE tz_utc(time timestamptz NOT NULL, value int);
SELECT create_hypertable('tz_utc', 'time', chunk_time_interval => interval '1 day');

INSERT INTO tz_utc VALUES
    ('2025-03-08 12:00:00', 1),
    ('2025-03-09 12:00:00', 2),
    ('2025-03-10 12:00:00', 3);

-- All chunks are 24 hours in UTC (no DST)
SELECT chunk_name, range_start, range_end,
       round(extract(epoch FROM (range_end - range_start)) / 3600, 1) AS hours
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_utc' ORDER BY range_start;

DROP TABLE tz_utc;

-- Create in UTC, then switch to DST timezone
-- Origin is UTC midnight, but chunk duration depends on session timezone when chunk is created
SET timezone = 'UTC';

CREATE TABLE tz_utc_then_dst(time timestamptz NOT NULL, value int);
SELECT create_hypertable('tz_utc_then_dst', 'time', chunk_time_interval => interval '1 day');

-- Now switch to NY and insert around DST transition
SET timezone = 'America/New_York';

INSERT INTO tz_utc_then_dst VALUES
    ('2025-03-08 12:00:00', 1),
    ('2025-03-09 12:00:00', 2),
    ('2025-03-10 12:00:00', 3);

-- Chunks are 23 hours around DST because interval addition uses session timezone
SELECT chunk_name, range_start, range_end,
       round(extract(epoch FROM (range_end - range_start)) / 3600, 1) AS hours
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_utc_then_dst' ORDER BY range_start;

-- Now change origin to local timezone (NY) midnight
-- This means new chunks will align to NY midnight instead of UTC midnight
SELECT set_chunk_time_interval('tz_utc_then_dst', interval '1 day',
    chunk_time_origin => '2001-01-01 00:00:00 America/New_York'::timestamptz);

-- Insert data that creates chunks aligned to NY midnight (not UTC midnight)
-- These chunks should align to local midnight
INSERT INTO tz_utc_then_dst VALUES
    ('2025-03-15 12:00:00', 4),  -- New chunk aligned to NY midnight
    ('2025-03-16 12:00:00', 5);  -- Another new chunk

-- Show all chunks - old ones aligned to UTC, new ones aligned to NY
SELECT chunk_name, range_start, range_end,
       round(extract(epoch FROM (range_end - range_start)) / 3600, 1) AS hours
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_utc_then_dst' ORDER BY range_start;

-- Now insert data that falls into the gap between old UTC-aligned and new NY-aligned chunks
-- This chunk will be "cut" to fit between existing chunks
INSERT INTO tz_utc_then_dst VALUES ('2025-03-10 23:00:00', 6);  -- Between existing chunks

-- Show all chunks with range size - the transition chunk should be smaller
SELECT chunk_name, range_start, range_end,
       round(extract(epoch FROM (range_end - range_start)) / 3600, 1) AS hours
FROM timescaledb_information.chunks WHERE hypertable_name = 'tz_utc_then_dst' ORDER BY range_start;

DROP TABLE tz_utc_then_dst;

-- Reset to Chatham for remaining tests
SET timezone = 'Pacific/Chatham';

---------------------------------------------------------------
-- HYPERTABLE TESTS: TIMESTAMP (without timezone)
---------------------------------------------------------------

CREATE TABLE ts_monthly(time timestamp NOT NULL, value int);
SELECT create_hypertable('ts_monthly', 'time', chunk_time_interval => interval '1 month');
INSERT INTO ts_monthly VALUES
    ('2025-01-15 12:00:00', 1),
    ('2025-02-15 12:00:00', 2),
    ('2025-03-15 12:00:00', 3);
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'ts_monthly' ORDER BY range_start;
DROP TABLE ts_monthly;

---------------------------------------------------------------
-- HYPERTABLE TESTS: DATE
---------------------------------------------------------------

CREATE TABLE date_monthly(day date NOT NULL, value int);
SELECT create_hypertable('date_monthly', 'day', chunk_time_interval => interval '1 month');
INSERT INTO date_monthly VALUES
    ('2025-01-15', 1),
    ('2025-02-15', 2),
    ('2025-03-15', 3);
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'date_monthly' ORDER BY range_start;
DROP TABLE date_monthly;

---------------------------------------------------------------
-- CUSTOM ORIGIN TESTS
---------------------------------------------------------------

-- Fiscal year starting July 1
CREATE TABLE fiscal_year(time timestamptz NOT NULL, value int);
SELECT create_hypertable('fiscal_year',
    by_range('time', interval '1 year', partition_origin => '2020-07-01 00:00:00 UTC'::timestamptz));

INSERT INTO fiscal_year VALUES
    ('2024-06-30 23:59:59 UTC', 1),  -- FY2024
    ('2024-07-01 00:00:00 UTC', 2),  -- FY2025
    ('2025-06-30 23:59:59 UTC', 3);  -- FY2025

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'fiscal_year' ORDER BY range_start;
DROP TABLE fiscal_year;

-- Quarters starting April 1
CREATE TABLE fiscal_quarter(time timestamptz NOT NULL, value int);
SELECT create_hypertable('fiscal_quarter',
    by_range('time', interval '3 months', partition_origin => '2024-04-01 00:00:00 UTC'::timestamptz));

INSERT INTO fiscal_quarter VALUES
    ('2024-03-31 23:59:59 UTC', 1),  -- Q4 FY2024
    ('2024-04-01 00:00:00 UTC', 2),  -- Q1 FY2025
    ('2024-07-01 00:00:00 UTC', 3),  -- Q2 FY2025
    ('2024-10-01 00:00:00 UTC', 4);  -- Q3 FY2025

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'fiscal_quarter' ORDER BY range_start;
DROP TABLE fiscal_quarter;

-- Days starting at noon
CREATE TABLE noon_days(time timestamptz NOT NULL, value int);
SELECT create_hypertable('noon_days', 'time',
    chunk_time_interval => interval '1 day',
    chunk_time_origin => '2024-06-01 12:00:00 UTC'::timestamptz);

INSERT INTO noon_days VALUES
    ('2024-06-01 00:00:00 UTC', 1),
    ('2024-06-01 11:59:59 UTC', 2),
    ('2024-06-01 12:00:00 UTC', 3),
    ('2024-06-02 11:59:59 UTC', 4);

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'noon_days' ORDER BY range_start;
DROP TABLE noon_days;

-- Partitioning function with interval and origin (int epoch seconds to timestamptz)
CREATE OR REPLACE FUNCTION epoch_sec_to_timestamptz(epoch_sec int)
RETURNS timestamptz LANGUAGE SQL IMMUTABLE AS $$
    SELECT to_timestamp(epoch_sec);
$$;

CREATE TABLE events_epoch(epoch_sec int NOT NULL, value int);
SELECT create_hypertable('events_epoch',
    by_range('epoch_sec', interval '1 month',
             partition_func => 'epoch_sec_to_timestamptz',
             partition_origin => '2024-07-01 00:00:00 UTC'::timestamptz));

-- Epoch values in seconds:
-- 2024-06-30 23:59:59 UTC = 1719791999
-- 2024-07-01 00:00:00 UTC = 1719792000
-- 2024-07-15 12:00:00 UTC = 1721044800
-- 2024-08-01 00:00:00 UTC = 1722470400
INSERT INTO events_epoch VALUES
    (1719791999, 1),
    (1719792000, 2),
    (1721044800, 3),
    (1722470400, 4);

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'events_epoch' ORDER BY range_start;
DROP TABLE events_epoch;
DROP FUNCTION epoch_sec_to_timestamptz(int);

---------------------------------------------------------------
-- ADD_DIMENSION TESTS
---------------------------------------------------------------

-- Test add_dimension() with origin parameter (deprecated API)
-- Add a time dimension to a table with integer primary dimension
CREATE TABLE add_dim_old_api(id int NOT NULL, time timestamptz NOT NULL, value int);
SELECT create_hypertable('add_dim_old_api', 'id', chunk_time_interval => 100);
SELECT add_dimension('add_dim_old_api', 'time',
    chunk_time_interval => interval '1 month',
    chunk_time_origin => '2024-04-01 00:00:00 UTC'::timestamptz);

INSERT INTO add_dim_old_api VALUES
    (1, '2024-03-31 23:59:59 UTC', 1),  -- March (before origin month)
    (1, '2024-04-15 12:00:00 UTC', 2),  -- April
    (1, '2024-05-15 12:00:00 UTC', 3);  -- May

-- Verify the origin was stored correctly in the dimension
SELECT d.column_name,
       _timescaledb_functions.to_timestamp(d.interval_origin) AS origin
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'add_dim_old_api' AND d.column_name = 'time';

DROP TABLE add_dim_old_api;

-- Test add_dimension() with by_range and partition_origin (new API)
CREATE TABLE add_dim_new_api(id int NOT NULL, time timestamptz NOT NULL, value int);
SELECT create_hypertable('add_dim_new_api', 'id', chunk_time_interval => 100);
SELECT add_dimension('add_dim_new_api',
    by_range('time', interval '3 months', partition_origin => '2024-07-01 00:00:00 UTC'::timestamptz));

INSERT INTO add_dim_new_api VALUES
    (1, '2024-06-30 23:59:59 UTC', 1),  -- Q2 (before origin)
    (1, '2024-07-01 00:00:00 UTC', 2),  -- Q3 (at origin)
    (1, '2024-10-01 00:00:00 UTC', 3);  -- Q4

-- Verify the origin was stored correctly
SELECT d.column_name,
       _timescaledb_functions.to_timestamp(d.interval_origin) AS origin
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'add_dim_new_api' AND d.column_name = 'time';

DROP TABLE add_dim_new_api;

---------------------------------------------------------------
-- SET_CHUNK_TIME_INTERVAL TESTS
---------------------------------------------------------------

-- Test set_chunk_time_interval() with origin parameter
SET timezone = 'UTC';

CREATE TABLE set_interval_test(time timestamptz NOT NULL, value int);
SELECT create_hypertable('set_interval_test', 'time', chunk_time_interval => interval '1 day');

-- Insert to create initial chunk aligned to default origin (midnight)
INSERT INTO set_interval_test VALUES ('2024-06-15 12:00:00 UTC', 1);

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'set_interval_test' ORDER BY range_start;

-- Verify current origin (default: 2001-01-01 midnight)
SELECT d.column_name,
       _timescaledb_functions.to_timestamp(d.interval_origin) AS origin
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'set_interval_test';

-- Verify calendar chunking is still enabled
SHOW timescaledb.enable_calendar_chunking;

-- Change interval and origin to noon
SELECT set_chunk_time_interval('set_interval_test', interval '1 day', chunk_time_origin => '2024-01-01 12:00:00 UTC'::timestamptz);

-- Verify the origin was updated to noon
SELECT d.column_name,
       _timescaledb_functions.to_timestamp(d.interval_origin) AS origin
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'set_interval_test';

-- Insert to create transition chunk from midnight to noon
-- This data falls after the existing chunk's end (June 16 00:00) but before
-- the next noon boundary (June 16 12:00), creating a 12-hour transition chunk
INSERT INTO set_interval_test VALUES ('2024-06-16 06:00:00 UTC', 2);

-- Insert to create new chunk aligned to noon origin
INSERT INTO set_interval_test VALUES ('2024-07-15 18:00:00 UTC', 3);

-- Old chunk starts at midnight, transition chunk is 12 hours, new chunk starts at noon
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'set_interval_test' ORDER BY range_start;
DROP TABLE set_interval_test;

-- Test set_chunk_time_interval() changing from days to months with fiscal origin
CREATE TABLE set_interval_fiscal(time timestamptz NOT NULL, value int);
SELECT create_hypertable('set_interval_fiscal', 'time', chunk_time_interval => interval '1 day');

-- Insert initial data
INSERT INTO set_interval_fiscal VALUES ('2024-06-15 12:00:00 UTC', 1);

-- Change to monthly interval with fiscal year origin (July 1)
SELECT set_chunk_time_interval('set_interval_fiscal', interval '1 month', chunk_time_origin => '2024-07-01 00:00:00 UTC'::timestamptz);

-- Verify the origin was updated
SELECT d.column_name,
       _timescaledb_functions.to_timestamp(d.interval_origin) AS origin
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'set_interval_fiscal';

-- Insert more data to create monthly chunks aligned to fiscal year
INSERT INTO set_interval_fiscal VALUES
    ('2024-07-15 12:00:00 UTC', 2),
    ('2024-08-15 12:00:00 UTC', 3);

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'set_interval_fiscal' ORDER BY range_start;
DROP TABLE set_interval_fiscal;

-- Reset timezone
SET timezone = 'Pacific/Chatham';

---------------------------------------------------------------
-- COMPARISON: CALENDAR VS NON-CALENDAR CHUNKING
---------------------------------------------------------------

-- Show what the UTC values look like in Chatham time zone
-- Chatham is UTC+13:45 in January (daylight saving), so noon UTC is 01:45 next day
SELECT '2025-01-01 12:00:00 UTC'::timestamptz AS "UTC noon Jan 1",
       '2025-01-02 12:00:00 UTC'::timestamptz AS "UTC noon Jan 2";

-- Non-calendar chunking
SET timescaledb.enable_calendar_chunking = false;

CREATE TABLE non_calendar(time timestamptz NOT NULL, value int);
SELECT create_hypertable('non_calendar', 'time', chunk_time_interval => interval '1 day');
INSERT INTO non_calendar VALUES
    ('2025-01-01 12:00:00 UTC', 1),
    ('2025-01-02 12:00:00 UTC', 2);
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'non_calendar' ORDER BY range_start;
DROP TABLE non_calendar;

-- Calendar chunking
SET timescaledb.enable_calendar_chunking = true;

CREATE TABLE calendar(time timestamptz NOT NULL, value int);
SELECT create_hypertable('calendar', 'time', chunk_time_interval => interval '1 day');
INSERT INTO calendar VALUES
    ('2025-01-01 12:00:00 UTC', 1),
    ('2025-01-02 12:00:00 UTC', 2);
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'calendar' ORDER BY range_start;
DROP TABLE calendar;

---------------------------------------------------------------
-- SET_CHUNK_TIME_INTERVAL WITH NON-INTERVAL TYPE
---------------------------------------------------------------
-- Test that passing integer (microseconds) instead of Interval
-- to set_chunk_time_interval() on a calendar-based hypertable errors.

SET timezone = 'UTC';
SET timescaledb.enable_calendar_chunking = true;

CREATE TABLE calendar_no_integer(time timestamptz NOT NULL, value int);
SELECT create_hypertable('calendar_no_integer', 'time', chunk_time_interval => interval '1 day');

-- Verify calendar chunking is active
SELECT d.column_name,
       d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'calendar_no_integer';

-- Try to change to integer interval - this should error
\set ON_ERROR_STOP 0
SELECT set_chunk_time_interval('calendar_no_integer', 86400000000::bigint);
\set ON_ERROR_STOP 1

-- Verify calendar chunking is still active (unchanged)
SELECT d.column_name,
       d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'calendar_no_integer';

-- But changing to another Interval value should work
SELECT set_chunk_time_interval('calendar_no_integer', interval '1 week');
SELECT d.column_name,
       d.interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'calendar_no_integer';

DROP TABLE calendar_no_integer;

---------------------------------------------------------------
-- GUC CHANGE AFTER HYPERTABLE CREATION
-- Test that chunking mode is sticky (doesn't change with GUC)
-- Test all combinations of:
--   - Hypertable type: calendar vs non-calendar
--   - GUC state: ON vs OFF
--   - Interval input: INTERVAL vs integer
---------------------------------------------------------------

SET timezone = 'UTC';

---------------------------------------------------------------
-- NON-CALENDAR HYPERTABLE TESTS
-- Created with calendar_chunking = OFF
-- Should stay non-calendar regardless of GUC or input type
---------------------------------------------------------------

SET timescaledb.enable_calendar_chunking = false;

CREATE TABLE non_calendar_ht(time timestamptz NOT NULL, value int);
SELECT create_hypertable('non_calendar_ht', 'time', chunk_time_interval => interval '1 day');

-- Verify initial state: non-calendar mode (has_integer_interval=t, has_time_interval=f)
SELECT d.column_name,
       d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval_length
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'non_calendar_ht';

-- GUC OFF + integer input → stays non-calendar
SET timescaledb.enable_calendar_chunking = false;
SELECT set_chunk_time_interval('non_calendar_ht', 172800000000::bigint);  -- 2 days in microseconds

SELECT d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval_length
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'non_calendar_ht';

-- GUC OFF + INTERVAL input → stays non-calendar (converted to microseconds)
SET timescaledb.enable_calendar_chunking = false;
SELECT set_chunk_time_interval('non_calendar_ht', interval '3 days');

SELECT d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval_length
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'non_calendar_ht';

-- GUC ON + integer input → stays non-calendar
SET timescaledb.enable_calendar_chunking = true;
SELECT set_chunk_time_interval('non_calendar_ht', 345600000000::bigint);  -- 4 days in microseconds

SELECT d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval_length
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'non_calendar_ht';

-- GUC ON + INTERVAL input → stays non-calendar (converted to microseconds)
SET timescaledb.enable_calendar_chunking = true;
SELECT set_chunk_time_interval('non_calendar_ht', interval '5 days');

SELECT d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval_length
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'non_calendar_ht';

DROP TABLE non_calendar_ht;

---------------------------------------------------------------
-- CALENDAR HYPERTABLE TESTS
-- Created with calendar_chunking = ON
-- Should stay calendar regardless of GUC
-- Integer input should always error
---------------------------------------------------------------

SET timescaledb.enable_calendar_chunking = true;

CREATE TABLE calendar_ht(time timestamptz NOT NULL, value int);
SELECT create_hypertable('calendar_ht', 'time', chunk_time_interval => interval '1 day');

-- Verify initial state: calendar mode (interval_length IS NULL, interval IS NOT NULL)
SELECT d.column_name,
       d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'calendar_ht';

-- GUC ON + INTERVAL input → stays calendar
SET timescaledb.enable_calendar_chunking = true;
SELECT set_chunk_time_interval('calendar_ht', interval '2 days');

SELECT d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'calendar_ht';

-- GUC OFF + INTERVAL input → stays calendar
SET timescaledb.enable_calendar_chunking = false;
SELECT set_chunk_time_interval('calendar_ht', interval '3 days');

SELECT d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'calendar_ht';

-- GUC ON + integer input → ERROR (cannot use integer on calendar hypertable)
SET timescaledb.enable_calendar_chunking = true;
\set ON_ERROR_STOP 0
SELECT set_chunk_time_interval('calendar_ht', 86400000000::bigint);
\set ON_ERROR_STOP 1

-- Verify unchanged after error
SELECT d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'calendar_ht';

-- GUC OFF + integer input → ERROR (cannot use integer on calendar hypertable)
SET timescaledb.enable_calendar_chunking = false;
\set ON_ERROR_STOP 0
SELECT set_chunk_time_interval('calendar_ht', 172800000000::bigint);
\set ON_ERROR_STOP 1

-- Verify unchanged after error
SELECT d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'calendar_ht';

DROP TABLE calendar_ht;

---------------------------------------------------------------
-- MODE SWITCHING TESTS
-- Test switching between calendar and non-calendar modes
-- using the calendar_chunking parameter
---------------------------------------------------------------

-- Use a non-UTC timezone to make calendar vs non-calendar differences visible
-- In UTC, chunks would align nicely even without calendar chunking
SET timezone = 'America/New_York';
SET timescaledb.enable_calendar_chunking = false;

CREATE TABLE mode_switch(time timestamptz NOT NULL, value int);
SELECT create_hypertable('mode_switch', 'time', chunk_time_interval => interval '1 day');

-- 1. Initial state: non-calendar mode
SELECT d.column_name,
       d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'mode_switch';

INSERT INTO mode_switch VALUES
    ('2025-01-01 12:00:00 UTC', 1),
    ('2025-01-02 12:00:00 UTC', 2);

-- Non-calendar: chunk boundaries at 19:00 EST (midnight UTC), not local midnight
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'mode_switch' ORDER BY range_start;

-- 2. Switch to calendar mode using set_chunk_time_interval
SELECT set_chunk_time_interval('mode_switch', '1 week'::interval, calendar_chunking => true);

SELECT d.column_name,
       d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'mode_switch';

INSERT INTO mode_switch VALUES
    ('2025-02-10 12:00:00 UTC', 3),
    ('2025-02-17 12:00:00 UTC', 4);

-- Calendar: new chunks aligned to local Monday midnight (00:00 EST)
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'mode_switch' ORDER BY range_start;

-- 3. Switch back to non-calendar using set_partitioning_interval
SELECT set_partitioning_interval('mode_switch', '2 days'::interval, calendar_chunking => false);

SELECT d.column_name,
       d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval_length
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'mode_switch';

INSERT INTO mode_switch VALUES
    ('2025-06-15 12:00:00 UTC', 5),
    ('2025-06-18 12:00:00 UTC', 6);

-- Non-calendar: fixed 2-day intervals, boundaries at odd hours
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'mode_switch' ORDER BY range_start;

-- 4. Switch back to calendar using set_chunk_time_interval
SELECT set_chunk_time_interval('mode_switch', '1 month'::interval, calendar_chunking => true);

SELECT d.column_name,
       d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval,
       d.interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'mode_switch';

INSERT INTO mode_switch VALUES
    ('2025-09-15 12:00:00 UTC', 7),
    ('2025-10-15 12:00:00 UTC', 8);

-- Calendar: new chunks aligned to local month start (00:00 EST on 1st)
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks WHERE hypertable_name = 'mode_switch' ORDER BY range_start;

DROP TABLE mode_switch;

-- Test error: calendar_chunking => true with integer interval
CREATE TABLE calendar_switch_error(time timestamptz NOT NULL, value int);
SELECT create_hypertable('calendar_switch_error', 'time', chunk_time_interval => interval '1 day');

\set ON_ERROR_STOP 0
SELECT set_chunk_time_interval('calendar_switch_error', 86400000000::bigint, calendar_chunking => true);
\set ON_ERROR_STOP 1

SELECT d.column_name,
       d.interval_length IS NOT NULL AS has_integer_interval,
       d.interval IS NOT NULL AS has_time_interval
FROM _timescaledb_catalog.dimension d
JOIN _timescaledb_catalog.hypertable h ON d.hypertable_id = h.id
WHERE h.table_name = 'calendar_switch_error';

DROP TABLE calendar_switch_error;

RESET timescaledb.enable_calendar_chunking;

---------------------------------------------------------------
-- CHUNK POSITION RELATIVE TO ORIGIN
-- Test chunks before, enclosing, and after the origin
---------------------------------------------------------------

SET timezone = 'UTC';
SET timescaledb.enable_calendar_chunking = true;

-- Test with origin in the middle of the data range
-- Origin: 2020-01-15 (middle of January)
CREATE TABLE origin_position(time timestamptz NOT NULL, value int);
SELECT create_hypertable('origin_position',
    by_range('time', interval '1 month', partition_origin => '2020-01-15 00:00:00 UTC'::timestamptz));

-- Insert data: before origin, enclosing origin, after origin
INSERT INTO origin_position VALUES
    -- Chunks BEFORE origin (historical data)
    ('2019-11-20 12:00:00 UTC', 1),  -- Nov 15 - Dec 15, 2019
    ('2019-12-20 12:00:00 UTC', 2),  -- Dec 15 - Jan 15, 2020
    -- Chunk ENCLOSING origin (origin is at chunk boundary)
    ('2020-01-15 00:00:00 UTC', 3),  -- Exactly at origin
    ('2020-01-20 12:00:00 UTC', 4),  -- Jan 15 - Feb 15, 2020
    -- Chunks AFTER origin
    ('2020-02-20 12:00:00 UTC', 5),  -- Feb 15 - Mar 15, 2020
    ('2020-03-20 12:00:00 UTC', 6);  -- Mar 15 - Apr 15, 2020

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'origin_position'
ORDER BY range_start;

-- Verify data is in correct chunks
SELECT tableoid::regclass as chunk, time, value
FROM origin_position ORDER BY time;

DROP TABLE origin_position;

-- Test with daily chunks and origin at noon
-- This tests chunks before/after the origin time-of-day boundary
CREATE TABLE origin_daily(time timestamptz NOT NULL, value int);
SELECT create_hypertable('origin_daily',
    by_range('time', interval '1 day', partition_origin => '2020-06-15 12:00:00 UTC'::timestamptz));

INSERT INTO origin_daily VALUES
    -- Before origin date (chunks before origin)
    ('2020-06-13 18:00:00 UTC', 1),  -- Jun 13 noon - Jun 14 noon
    ('2020-06-14 18:00:00 UTC', 2),  -- Jun 14 noon - Jun 15 noon
    -- At and after origin
    ('2020-06-15 12:00:00 UTC', 3),  -- Exactly at origin
    ('2020-06-15 18:00:00 UTC', 4),  -- Jun 15 noon - Jun 16 noon
    ('2020-06-16 06:00:00 UTC', 5),  -- Same chunk (before noon)
    ('2020-06-16 18:00:00 UTC', 6);  -- Jun 16 noon - Jun 17 noon

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'origin_daily'
ORDER BY range_start;

DROP TABLE origin_daily;

---------------------------------------------------------------
-- EXTREME VALUES AND OVERFLOW CLAMPING TESTS
-- Test timestamps near the boundaries of the valid range
---------------------------------------------------------------

-- Test calc_range with ancient timestamps
-- PostgreSQL timestamp range: 4713 BC to 294276 AD

-- Ancient timestamp tests
SELECT *, end_ts - start_ts AS range_size FROM calc_range('4700-01-15 00:00:00 BC'::timestamptz, '1 year'::interval);
SELECT *, end_ts - start_ts AS range_size FROM calc_range('4700-01-15 00:00:00 BC'::timestamptz, '1 month'::interval);
SELECT *, end_ts - start_ts AS range_size FROM calc_range('4700-01-15 00:00:00 BC'::timestamptz, '1 day'::interval);

-- Far future timestamp tests (but within valid range)
SELECT *, end_ts - start_ts AS range_size FROM calc_range('100000-01-15 00:00:00'::timestamptz, '1 year'::interval);
SELECT *, end_ts - start_ts AS range_size FROM calc_range('100000-01-15 00:00:00'::timestamptz, '1 month'::interval);
SELECT *, end_ts - start_ts AS range_size FROM calc_range('100000-01-15 00:00:00'::timestamptz, '1 day'::interval);

-- Test hypertable with data spanning ancient times
CREATE TABLE extreme_ancient(time timestamptz NOT NULL, value int);
SELECT create_hypertable('extreme_ancient', 'time', chunk_time_interval => interval '100 years');

INSERT INTO extreme_ancient VALUES
    ('4700-06-15 00:00:00 BC', 1),
    ('4600-06-15 00:00:00 BC', 2);

-- Verify chunks are created for ancient dates
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'extreme_ancient'
ORDER BY range_start;

-- Show CHECK constraints on chunks with chunk size
-- Chunk size should be close to 100 years when not clamped
SELECT * FROM show_chunk_constraints('extreme_ancient');

DROP TABLE extreme_ancient;

-- Test hypertable with data in far future
CREATE TABLE extreme_future(time timestamptz NOT NULL, value int);
SELECT create_hypertable('extreme_future', 'time', chunk_time_interval => interval '100 years');

INSERT INTO extreme_future VALUES
    ('100000-06-15 00:00:00', 1),
    ('100100-06-15 00:00:00', 2);

-- Verify chunks are created for far future dates
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'extreme_future'
ORDER BY range_start;

-- Show CHECK constraints on chunks with chunk size
-- Chunk size should be close to 100 years when not clamped
SELECT * FROM show_chunk_constraints('extreme_future');

DROP TABLE extreme_future;

-- Test chunks with open-ended constraints (clamped to MIN/MAX)
-- Use large intervals that cause boundaries to overflow
CREATE TABLE open_ended_chunks(time timestamptz NOT NULL, value int);
SELECT create_hypertable('open_ended_chunks', 'time', chunk_time_interval => interval '10000 years');

-- Insert at boundaries - should create open-ended constraints
INSERT INTO open_ended_chunks VALUES
    ('4700-06-15 00:00:00 BC', 1),  -- Near min boundary, chunk start overflows
    ('294000-06-15 00:00:00', 2);   -- Near max boundary, chunk end overflows

-- Show CHECK constraints - should see open-ended constraints
-- Chunk size should be smaller than 10000 years when clamped
SELECT * FROM show_chunk_constraints('open_ended_chunks');

DROP TABLE open_ended_chunks;

-- Test with origin far in the past - chunks before and after
CREATE TABLE origin_ancient(time timestamptz NOT NULL, value int);
SELECT create_hypertable('origin_ancient',
    by_range('time', interval '1000 years', partition_origin => '2000-01-01 00:00:00 BC'::timestamptz));

INSERT INTO origin_ancient VALUES
    ('4000-06-15 00:00:00 BC', 1),  -- Before origin
    ('1500-06-15 00:00:00 BC', 2),  -- After origin (closer to present)
    ('500-06-15 00:00:00', 3),      -- After origin (AD)
    ('2020-06-15 00:00:00', 4);     -- Modern times

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'origin_ancient'
ORDER BY range_start;

DROP TABLE origin_ancient;

-- Test with origin far in the future - all data before origin
CREATE TABLE origin_future(time timestamptz NOT NULL, value int);
SELECT create_hypertable('origin_future',
    by_range('time', interval '100 years', partition_origin => '50000-01-01 00:00:00'::timestamptz));

INSERT INTO origin_future VALUES
    ('2020-06-15 00:00:00', 1),
    ('2120-06-15 00:00:00', 2),
    ('2220-06-15 00:00:00', 3);

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'origin_future'
ORDER BY range_start;

DROP TABLE origin_future;

---------------------------------------------------------------
-- MIXED INTERVAL TESTS WITH EXTREME VALUES
-- Test intervals with month+day+time components near boundaries
---------------------------------------------------------------

-- Mixed interval (month + day) with values spanning origin
SELECT * FROM calc_range('2020-01-15 00:00:00 UTC'::timestamptz, '1 month 15 days'::interval,
                         '2020-02-01 00:00:00 UTC'::timestamptz);
SELECT * FROM calc_range('2020-02-01 00:00:00 UTC'::timestamptz, '1 month 15 days'::interval,
                         '2020-02-01 00:00:00 UTC'::timestamptz);
SELECT * FROM calc_range('2020-03-01 00:00:00 UTC'::timestamptz, '1 month 15 days'::interval,
                         '2020-02-01 00:00:00 UTC'::timestamptz);

-- Mixed interval with ancient dates
SELECT *, end_ts - start_ts AS range_size FROM calc_range('4700-01-15 00:00:00 BC'::timestamptz, '1 month 15 days'::interval);
SELECT *, end_ts - start_ts AS range_size FROM calc_range('50000-01-15 00:00:00'::timestamptz, '1 month 15 days'::interval);

-- Create hypertable with mixed interval
CREATE TABLE mixed_interval(time timestamptz NOT NULL, value int);
SELECT create_hypertable('mixed_interval',
    by_range('time', interval '1 month 15 days', partition_origin => '2020-01-01 00:00:00 UTC'::timestamptz));

INSERT INTO mixed_interval VALUES
    ('2019-12-01 00:00:00 UTC', 1),  -- Before origin
    ('2020-01-01 00:00:00 UTC', 2),  -- At origin
    ('2020-01-20 00:00:00 UTC', 3),  -- Same chunk as origin
    ('2020-02-20 00:00:00 UTC', 4),  -- Next chunk
    ('2020-04-01 00:00:00 UTC', 5);  -- Two chunks after

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'mixed_interval'
ORDER BY range_start;

DROP TABLE mixed_interval;

---------------------------------------------------------------
-- INTEGER DIMENSION OVERFLOW TESTS
-- Test integer dimensions with values near INT64 boundaries
---------------------------------------------------------------

-- Test bigint with large positive values
CREATE TABLE int_extreme_max(time bigint NOT NULL, value int);
SELECT create_hypertable('int_extreme_max', by_range('time', 1000000000000, partition_origin => 0));

INSERT INTO int_extreme_max VALUES
    (9223372036854775000, 1),  -- Near INT64_MAX
    (9223372036854774000, 2);

SELECT * FROM show_int_chunk_slices('int_extreme_max');

-- Show CHECK constraints - should see open-ended constraint (no upper bound)
SELECT * FROM show_check_constraints('int_extreme_max');

DROP TABLE int_extreme_max;

-- Test bigint with large negative values
CREATE TABLE int_extreme_min(time bigint NOT NULL, value int);
SELECT create_hypertable('int_extreme_min', by_range('time', 1000000000000, partition_origin => 0));

INSERT INTO int_extreme_min VALUES
    (-9223372036854775000, 1),  -- Near INT64_MIN
    (-9223372036854774000, 2);

SELECT * FROM show_int_chunk_slices('int_extreme_min');

-- Show CHECK constraints - should see open-ended constraint (no lower bound)
SELECT * FROM show_check_constraints('int_extreme_min');

DROP TABLE int_extreme_min;

-- Test integer origin with values on both sides
CREATE TABLE int_origin_sides(time bigint NOT NULL, value int);
SELECT create_hypertable('int_origin_sides', by_range('time', 100, partition_origin => 1000));

INSERT INTO int_origin_sides VALUES
    (850, 1),   -- Chunk [800, 900)
    (950, 2),   -- Chunk [900, 1000)
    (1000, 3),  -- At origin, chunk [1000, 1100)
    (1050, 4),  -- Same chunk as origin
    (1100, 5),  -- Chunk [1100, 1200)
    (1150, 6);  -- Same chunk

SELECT * FROM show_int_chunk_slices('int_origin_sides');

-- Verify data placement
SELECT tableoid::regclass as chunk, time, value
FROM int_origin_sides ORDER BY time;

DROP TABLE int_origin_sides;

RESET timezone;
RESET timescaledb.enable_calendar_chunking;

---------------------------------------------------------------
-- TIMESTAMP WITHOUT TIMEZONE AND GENERAL ALGORITHM TESTS
---------------------------------------------------------------
-- Test ts_chunk_range_calculate_general() with timestamp (without timezone)
-- and boundary/overflow values for improved test coverage

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Create wrapper function for timestamp without timezone
CREATE OR REPLACE FUNCTION calc_range_ts(ts TIMESTAMP, chunk_interval INTERVAL, origin TIMESTAMP DEFAULT NULL, force_general BOOL DEFAULT NULL)
RETURNS TABLE(start_ts TIMESTAMP, end_ts TIMESTAMP) AS :MODULE_PATHNAME, 'ts_dimension_calculate_open_range_calendar' LANGUAGE C;

SET ROLE :ROLE_DEFAULT_PERM_USER;
SET timescaledb.enable_calendar_chunking = true;

-- Test timestamp without timezone using general algorithm (force_general=true)
-- Covers various intervals and boundary values in a single query
SELECT t.ts, i.inv, r.start_ts, r.end_ts, r.end_ts - r.start_ts AS range_size
FROM unnest(ARRAY[
    '2025-03-15 12:00:00'::timestamp,
    '4700-01-15 00:00:00 BC'::timestamp,
    '100000-01-15 00:00:00'::timestamp
]) AS t(ts)
CROSS JOIN unnest(ARRAY[
    '1 day'::interval,
    '1 month'::interval,
    '1 year'::interval,
    '100 years'::interval,
    '1 month 15 days'::interval
]) AS i(inv)
CROSS JOIN LATERAL calc_range_ts(t.ts, i.inv, NULL, true) r
ORDER BY t.ts, i.inv;

-- Test timestamptz with general algorithm for comparison
SELECT t.ts, i.inv, r.start_ts, r.end_ts, r.end_ts - r.start_ts AS range_size
FROM unnest(ARRAY[
    '2025-03-15 12:00:00 UTC'::timestamptz,
    '4700-01-15 00:00:00 BC'::timestamptz,
    '100000-01-15 00:00:00'::timestamptz
]) AS t(ts)
CROSS JOIN unnest(ARRAY[
    '1 day'::interval,
    '1 month'::interval,
    '1 year'::interval,
    '100 years'::interval,
    '1 month 15 days'::interval
]) AS i(inv)
CROSS JOIN LATERAL calc_range(t.ts, i.inv, NULL, true) r
ORDER BY t.ts, i.inv;

-- Large interval test (1000 years) - separate due to potential overflow
SELECT *, end_ts - start_ts AS range_size FROM calc_range_ts('2025-03-15 12:00:00'::timestamp, '1000 years'::interval, NULL, true);
SELECT *, end_ts - start_ts AS range_size FROM calc_range('2025-03-15 00:00:00 UTC'::timestamptz, '1000 years'::interval, NULL, true);

-- Cleanup
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP FUNCTION calc_range_ts(TIMESTAMP, INTERVAL, TIMESTAMP, BOOL);
SET ROLE :ROLE_DEFAULT_PERM_USER;

RESET timescaledb.enable_calendar_chunking;

---------------------------------------------------------------
-- ERROR PATH TESTS
-- Test error handling for invalid timestamp values
-- These test the timestamp_datum_to_tm error paths in calc_month_chunk_range
-- and calc_day_chunk_range which trigger "timestamp out of range" errors.
---------------------------------------------------------------

-- Test month interval with infinity timestamp (triggers timestamp out of range)
\set ON_ERROR_STOP 0
SELECT * FROM calc_range('infinity'::timestamptz, '1 month'::interval);
SELECT * FROM calc_range('-infinity'::timestamptz, '1 month'::interval);

-- Test month interval with infinity origin (triggers origin timestamp out of range)
SELECT * FROM calc_range('2025-01-15'::timestamptz, '1 month'::interval, 'infinity'::timestamptz);
SELECT * FROM calc_range('2025-01-15'::timestamptz, '1 month'::interval, '-infinity'::timestamptz);

-- Test day interval with infinity timestamp
SELECT * FROM calc_range('infinity'::timestamptz, '1 day'::interval);
SELECT * FROM calc_range('-infinity'::timestamptz, '1 day'::interval);

-- Test day interval with infinity origin
SELECT * FROM calc_range('2025-01-15'::timestamptz, '1 day'::interval, 'infinity'::timestamptz);
SELECT * FROM calc_range('2025-01-15'::timestamptz, '1 day'::interval, '-infinity'::timestamptz);

-- Test sub-day interval with infinity values (these go through calc_sub_day_chunk_range)
SELECT * FROM calc_range('infinity'::timestamptz, '1 hour'::interval);
SELECT * FROM calc_range('-infinity'::timestamptz, '1 hour'::interval);
SELECT * FROM calc_range('2025-01-15'::timestamptz, '1 hour'::interval, 'infinity'::timestamptz);
SELECT * FROM calc_range('2025-01-15'::timestamptz, '1 hour'::interval, '-infinity'::timestamptz);
\set ON_ERROR_STOP 1

-- Test large intervals that produce chunk boundaries outside PostgreSQL's timestamp range
-- These verify that tm2timestamp failures are handled by clamping to -infinity/+infinity

-- Large month interval: 2 billion months (~166 million years) - chunk end exceeds max timestamp
SELECT * FROM calc_range('2025-01-15'::timestamptz, '2000000000 months'::interval);

-- Large day interval: 2 billion days (~5.4 million years) - chunk end exceeds max timestamp
SELECT * FROM calc_range('2025-01-15'::timestamptz, '2000000000 days'::interval);

-- Large month interval with ancient origin - chunk end exceeds max timestamp
SELECT * FROM calc_range('290000-01-15'::timestamptz, '200000000 months'::interval, '4000-01-01 BC'::timestamptz);

-- Large day interval with future origin - chunk start precedes min timestamp
SELECT * FROM calc_range('4000-01-15 BC'::timestamptz, '100000000 days'::interval, '290000-01-01'::timestamptz);

-- Test INT32 overflow in end_julian calculation: start_julian (~108M) + interval->day (~2.1B) > INT32_MAX
-- This triggers pg_add_s32_overflow in calc_day_chunk_range for end_julian
SELECT * FROM calc_range('290000-01-15'::timestamptz, '2100000000 days'::interval, '290000-01-01'::timestamptz);

-- Test INT32 overflow in end_total_months_from_jan: total_months_from_jan + interval->month > INT32_MAX
-- This triggers pg_add_s32_overflow in calc_month_chunk_range for end_total_months_from_jan
-- Use origin in December so total_months_from_jan starts at 11, making 11 + 2147483647 overflow
SELECT * FROM calc_range('2025-01-15'::timestamptz, '2147483647 months'::interval, '2001-12-01'::timestamptz);

-- Test sub-day interval overflow: ts_val - origin_val can overflow INT64 when timestamps are at opposite extremes
-- This triggers pg_sub_s64_overflow in calc_sub_day_chunk_range
\set ON_ERROR_STOP 0
SELECT * FROM calc_range('4700-01-15 BC'::timestamptz, '1 hour'::interval, '294000-01-01'::timestamptz);
\set ON_ERROR_STOP 1

---------------------------------------------------------------
-- C UNIT TESTS
-- Test saturating arithmetic and chunk range calculation
---------------------------------------------------------------
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT test_chunk_range();
SET ROLE :ROLE_DEFAULT_PERM_USER;

---------------------------------------------------------------
-- CLEANUP
---------------------------------------------------------------

DROP FUNCTION test_ranges(TIMESTAMPTZ[], INTERVAL[]);
DROP FUNCTION test_ranges_with_origin(TIMESTAMPTZ[], INTERVAL[], TIMESTAMPTZ);
DROP FUNCTION show_chunk_constraints(text);
DROP FUNCTION show_int_chunk_slices(text);
DROP FUNCTION show_check_constraints(text);
RESET timezone;
RESET timescaledb.enable_calendar_chunking;
\set VERBOSITY default
