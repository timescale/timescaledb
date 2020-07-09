-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Disable background workers since we are testing manual refresh
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_internal.stop_background_workers();
SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE TABLE conditions (time timestamptz NOT NULL, device int, temp float);
SELECT create_hypertable('conditions', 'time');

SELECT setseed(.12);

INSERT INTO conditions
SELECT t, ceil(abs(timestamp_hash(t::timestamp))%4)::int, abs(timestamp_hash(t::timestamp))%40
FROM generate_series('2020-05-01', '2020-05-05', '10 minutes'::interval) t;

-- Show the most recent data
SELECT * FROM conditions
ORDER BY time DESC, device
LIMIT 10;

CREATE VIEW daily_temp
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('1 day', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2;

-- The continuous aggregate should be empty
SELECT * FROM daily_temp
ORDER BY day DESC, device;

-- Refresh the most recent few days:
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-03', '2020-05-05');

SELECT * FROM daily_temp
ORDER BY day DESC, device;

-- Refresh the rest
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-01', '2020-05-03');

-- Compare the aggregate to the equivalent query on the source table
SELECT * FROM daily_temp
ORDER BY day DESC, device;

SELECT time_bucket('1 day', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2
ORDER BY 1 DESC,2;

-- Test unusual, but valid input
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-01'::timestamptz, '2020-05-03'::date);
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-01'::date, '2020-05-03'::date);
SELECT refresh_continuous_aggregate('daily_temp', 0, '2020-05-01');

-- Unbounded window forward in time
\set ON_ERROR_STOP 0
-- Currently doesn't work due to timestamp overflow bug in a query optimization
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-03', NULL);
SELECT refresh_continuous_aggregate('daily_temp', NULL, NULL);
\set ON_ERROR_STOP 1

-- Unbounded window back in time
SELECT refresh_continuous_aggregate('daily_temp', NULL, '2020-05-01');

-- Test bad input
\set ON_ERROR_STOP 0
-- Bad continuous aggregate name
SELECT refresh_continuous_aggregate(NULL, '2020-05-03', '2020-05-05');
SELECT refresh_continuous_aggregate('xyz', '2020-05-03', '2020-05-05');
-- Valid object, but not a continuous aggregate
SELECT refresh_continuous_aggregate('conditions', '2020-05-03', '2020-05-05');
-- Object ID with no object
SELECT refresh_continuous_aggregate(1, '2020-05-03', '2020-05-05');
-- Lacking arguments
SELECT refresh_continuous_aggregate('daily_temp');
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-03');
-- Bad time ranges
SELECT refresh_continuous_aggregate('daily_temp', 'xyz', '2020-05-05');
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-03', 'xyz');
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-03', '2020-05-01');
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-03', '2020-05-03');
-- Bad time input
SELECT refresh_continuous_aggregate('daily_temp', '2020-05-01'::text, '2020-05-03'::text);

\set ON_ERROR_STOP 1

-- Test different time types
CREATE TABLE conditions_date (time date NOT NULL, device int, temp float);
SELECT create_hypertable('conditions_date', 'time');

CREATE VIEW daily_temp_date
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('1 day', time) AS day, device, avg(temp) AS avg_temp
FROM conditions_date
GROUP BY 1,2;

SELECT refresh_continuous_aggregate('daily_temp_date', '2020-05-01', '2020-05-03');

CREATE TABLE conditions_int (time int NOT NULL, device int, temp float);
SELECT create_hypertable('conditions_int', 'time', chunk_time_interval => 10);

CREATE OR REPLACE FUNCTION integer_now_conditions()
RETURNS int LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM conditions_int
$$;

SELECT set_integer_now_func('conditions_int', 'integer_now_conditions');

CREATE VIEW daily_temp_int
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(4, time) AS day, device, avg(temp) AS avg_temp
FROM conditions_int
GROUP BY 1,2;

SELECT refresh_continuous_aggregate('daily_temp_int', 5, 10);
