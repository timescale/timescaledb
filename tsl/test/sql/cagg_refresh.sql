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

CREATE MATERIALIZED VIEW daily_temp
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('1 day', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH NO DATA;

-- The continuous aggregate should be empty
SELECT * FROM daily_temp
ORDER BY day DESC, device;

-- Refresh one bucket (1 day):
SHOW timezone;
-- The refresh of a single bucket must align with the start of the day
-- in the bucket's time zone (which is UTC, since time_bucket doesn't
-- support time zone arg)
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03 00:00 UTC', '2020-05-04 00:00 UTC');
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03 17:00 PDT', '2020-05-04 17:00 PDT');

\set ON_ERROR_STOP 0
\set VERBOSITY default
-- These refreshes will fail since they don't align with the bucket's
-- time zone
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03', '2020-05-04');
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03 00:00 PDT', '2020-05-04 00:00 PDT');

-- Refresh window less than one bucket
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03 00:00 UTC', '2020-05-03 23:59 UTC');
-- Refresh window bigger than one bucket, but failing since it is not
-- aligned with bucket boundaries so that it covers a full bucket:
--
-- Refresh window:    [----------)
-- Buckets:          [------|------]
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03 01:00 UTC', '2020-05-04 08:00 UTC');
\set VERBOSITY terse
\set ON_ERROR_STOP 1

-- Refresh the most recent few days:
CALL refresh_continuous_aggregate('daily_temp', '2020-05-02', '2020-05-05 17:00');

SELECT * FROM daily_temp
ORDER BY day DESC, device;

-- Refresh the rest (and try DEBUG output)
SET client_min_messages TO DEBUG1;
CALL refresh_continuous_aggregate('daily_temp', '2020-04-30', '2020-05-04');
RESET client_min_messages;

-- Compare the aggregate to the equivalent query on the source table
SELECT * FROM daily_temp
ORDER BY day DESC, device;

SELECT time_bucket('1 day', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2
ORDER BY 1 DESC,2;

-- Test unusual, but valid input
CALL refresh_continuous_aggregate('daily_temp', '2020-05-01'::timestamptz, '2020-05-03'::date);
CALL refresh_continuous_aggregate('daily_temp', '2020-05-01'::date, '2020-05-03'::date);

-- Unbounded window forward in time
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03', NULL);
CALL refresh_continuous_aggregate('daily_temp', NULL, NULL);

-- Unbounded window back in time
CALL refresh_continuous_aggregate('daily_temp', NULL, '2020-05-01');

-- Test bad input
\set ON_ERROR_STOP 0
-- Bad continuous aggregate name
CALL refresh_continuous_aggregate(NULL, '2020-05-03', '2020-05-05');
CALL refresh_continuous_aggregate('xyz', '2020-05-03', '2020-05-05');
-- Valid object, but not a continuous aggregate
CALL refresh_continuous_aggregate('conditions', '2020-05-03', '2020-05-05');
-- Object ID with no object
CALL refresh_continuous_aggregate(1, '2020-05-03', '2020-05-05');
-- Lacking arguments
CALL refresh_continuous_aggregate('daily_temp');
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03');
-- Bad time ranges
CALL refresh_continuous_aggregate('daily_temp', 'xyz', '2020-05-05');
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03', 'xyz');
CALL refresh_continuous_aggregate('daily_temp', '2020-05-03', '2020-05-01');
-- Bad time input
CALL refresh_continuous_aggregate('daily_temp', '2020-05-01'::text, '2020-05-03'::text);
CALL refresh_continuous_aggregate('daily_temp', 0, '2020-05-01');
\set ON_ERROR_STOP 1

-- Test different time types
CREATE TABLE conditions_date (time date NOT NULL, device int, temp float);
SELECT create_hypertable('conditions_date', 'time');

CREATE MATERIALIZED VIEW daily_temp_date
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('1 day', time) AS day, device, avg(temp) AS avg_temp
FROM conditions_date
GROUP BY 1,2 WITH NO DATA;

CALL refresh_continuous_aggregate('daily_temp_date', '2020-05-01', '2020-05-03');

-- Try max refresh window size
CALL refresh_continuous_aggregate('daily_temp_date', NULL, NULL);

-- Test smallint-based continuous aggregate
CREATE TABLE conditions_smallint (time smallint NOT NULL, device int, temp float);
SELECT create_hypertable('conditions_smallint', 'time', chunk_time_interval => 20);

INSERT INTO conditions_smallint
SELECT t, ceil(abs(timestamp_hash(to_timestamp(t)::timestamp))%4)::smallint, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
FROM generate_series(1, 100, 1) t;

CREATE OR REPLACE FUNCTION smallint_now()
RETURNS smallint LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)::smallint
    FROM conditions_smallint
$$;

\set ON_ERROR_STOP 0
-- First try to create an integer-based continuous aggregate without
-- an now function. This should not be allowed.
CREATE MATERIALIZED VIEW cond_20_smallint
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(SMALLINT '20', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions_smallint c
GROUP BY 1,2 WITH NO DATA;
\set ON_ERROR_STOP 1

SELECT set_integer_now_func('conditions_smallint', 'smallint_now');

CREATE MATERIALIZED VIEW cond_20_smallint
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(SMALLINT '20', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions_smallint c
GROUP BY 1,2 WITH NO DATA;

CALL refresh_continuous_aggregate('cond_20_smallint', 0::smallint, 70::smallint);

SELECT * FROM cond_20_smallint
ORDER BY 1,2;

-- Try max refresh window size
CALL refresh_continuous_aggregate('cond_20_smallint', NULL, NULL);

-- Test int-based continuous aggregate
CREATE TABLE conditions_int (time int NOT NULL, device int, temp float);
SELECT create_hypertable('conditions_int', 'time', chunk_time_interval => 20);

INSERT INTO conditions_int
SELECT t, ceil(abs(timestamp_hash(to_timestamp(t)::timestamp))%4)::int, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
FROM generate_series(1, 100, 1) t;

CREATE OR REPLACE FUNCTION int_now()
RETURNS int LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)
    FROM conditions_int
$$;

SELECT set_integer_now_func('conditions_int', 'int_now');

CREATE MATERIALIZED VIEW cond_20_int
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(INT '20', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions_int
GROUP BY 1,2 WITH NO DATA;

CALL refresh_continuous_aggregate('cond_20_int', 0, 65);

SELECT * FROM cond_20_int
ORDER BY 1,2;

-- Try max refresh window size
CALL refresh_continuous_aggregate('cond_20_int', NULL, NULL);

-- Test bigint-based continuous aggregate
CREATE TABLE conditions_bigint (time bigint NOT NULL, device int, temp float);
SELECT create_hypertable('conditions_bigint', 'time', chunk_time_interval => 20);

INSERT INTO conditions_bigint
SELECT t, ceil(abs(timestamp_hash(to_timestamp(t)::timestamp))%4)::bigint, abs(timestamp_hash(to_timestamp(t)::timestamp))%40
FROM generate_series(1, 100, 1) t;

CREATE OR REPLACE FUNCTION bigint_now()
RETURNS bigint LANGUAGE SQL STABLE AS
$$
    SELECT coalesce(max(time), 0)::bigint
    FROM conditions_bigint
$$;

SELECT set_integer_now_func('conditions_bigint', 'bigint_now');

CREATE MATERIALIZED VIEW cond_20_bigint
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket(BIGINT '20', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions_bigint
GROUP BY 1,2 WITH NO DATA;

CALL refresh_continuous_aggregate('cond_20_bigint', 0, 75);

SELECT * FROM cond_20_bigint
ORDER BY 1,2;

-- Try max refresh window size
CALL refresh_continuous_aggregate('cond_20_bigint', NULL, NULL);

-- Test that WITH NO DATA and WITH DATA works (we use whatever is the
-- default for Postgres, so we do not need to have test for the
-- default).

CREATE MATERIALIZED VIEW weekly_temp_without_data
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('7 days', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH NO DATA;

CREATE MATERIALIZED VIEW weekly_temp_with_data
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('7 days', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH DATA;

SELECT * FROM weekly_temp_without_data;
SELECT * FROM weekly_temp_with_data ORDER BY 1,2;

\set ON_ERROR_STOP 0
-- REFRESH MATERIALIZED VIEW is blocked on continuous aggregates
REFRESH MATERIALIZED VIEW weekly_temp_without_data;

-- These should fail since we do not allow refreshing inside a
-- transaction, not even as part of CREATE MATERIALIZED VIEW.
DO LANGUAGE PLPGSQL $$ BEGIN
CREATE MATERIALIZED VIEW weekly_conditions
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('7 days', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH DATA;
END $$;

BEGIN;
CREATE MATERIALIZED VIEW weekly_conditions
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('7 days', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH DATA;
COMMIT;

\set ON_ERROR_STOP 1

-- This should not fail since we do not refresh the continuous
-- aggregate.
DO LANGUAGE PLPGSQL $$ BEGIN
CREATE MATERIALIZED VIEW weekly_conditions_1
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('7 days', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH NO DATA;
END $$;

BEGIN;
CREATE MATERIALIZED VIEW weekly_conditions_2
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS
SELECT time_bucket('7 days', time) AS day, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH NO DATA;
COMMIT;
