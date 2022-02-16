-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE conditions(
  day DATE NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions', 'day',
  chunk_time_interval => INTERVAL '1 day'
);

INSERT INTO conditions (day, city, temperature) VALUES
  ('2021-06-14', 'Moscow', 26),
  ('2021-06-15', 'Moscow', 22),
  ('2021-06-16', 'Moscow', 24),
  ('2021-06-17', 'Moscow', 24),
  ('2021-06-18', 'Moscow', 27),
  ('2021-06-19', 'Moscow', 28),
  ('2021-06-20', 'Moscow', 30),
  ('2021-06-21', 'Moscow', 31),
  ('2021-06-22', 'Moscow', 34),
  ('2021-06-23', 'Moscow', 34),
  ('2021-06-24', 'Moscow', 34),
  ('2021-06-25', 'Moscow', 32),
  ('2021-06-26', 'Moscow', 32),
  ('2021-06-27', 'Moscow', 31);

\set ON_ERROR_STOP 0

-- Make sure NULL can't be specified as an origin
CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('7 days', day, null) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;

-- Make sure 'infinity' can't be specified as an origin
CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('7 days', day, 'infinity' :: date) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;

-- For monthly buckets origin should be the first day of the month
CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('1 month', day, '2021-06-03') AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;

-- Make sure buckets like '1 months 15 days" (fixed+variable-sized) are not allowed
CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('1 month 15 days', day, '2021-06-01') AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;

\set ON_ERROR_STOP 1

CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('7 days', day, '2000-01-03' :: date) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;

SELECT to_char(bucket, 'YYYY-MM-DD'), city, min, max
FROM conditions_summary_weekly
ORDER BY bucket;

SELECT mat_hypertable_id AS cagg_id, raw_hypertable_id AS ht_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'conditions_summary_weekly'
\gset

-- Make sure this is treated as a variable-sized bucket case
SELECT bucket_width
FROM _timescaledb_catalog.continuous_agg
WHERE mat_hypertable_id = :cagg_id;

-- Make sure the origin is saved in the catalog table
SELECT experimental, name, bucket_width, origin, timezone
FROM _timescaledb_catalog.continuous_aggs_bucket_function
WHERE mat_hypertable_id = :cagg_id;

-- Make sure truncating of the refresh window works
\set ON_ERROR_STOP 0
CALL refresh_continuous_aggregate('conditions_summary_weekly', '2021-06-14', '2021-06-20');
\set ON_ERROR_STOP 1

-- Make sure refreshing works
CALL refresh_continuous_aggregate('conditions_summary_weekly', '2021-06-14', '2021-06-21');
SELECT city, to_char(bucket, 'YYYY-MM-DD') AS week, min, max
FROM conditions_summary_weekly
ORDER BY week, city;

-- Check the invalidation threshold
SELECT _timescaledb_internal.to_timestamp(watermark) at time zone 'UTC'
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Add some dummy data for two more weeks and call refresh (no invalidations test case)
INSERT INTO conditions (day, city, temperature)
SELECT ts :: date, city, row_number() OVER ()
FROM generate_series('2021-06-28' :: date, '2021-07-11', '1 day') as ts,
     unnest(array['Moscow', 'Berlin']) as city;

-- Double check generated data
SELECT to_char(day, 'YYYY-MM-DD'), city, temperature
FROM conditions
WHERE day >= '2021-06-28'
ORDER BY city DESC, day;

-- Make sure the invalidation threshold was unaffected
SELECT _timescaledb_internal.to_timestamp(watermark) at time zone 'UTC'
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Make sure the invalidation log is empty
SELECT
    _timescaledb_internal.to_timestamp(lowest_modified_value) AS lowest,
    _timescaledb_internal.to_timestamp(greatest_modified_value) AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

-- Call refresh
CALL refresh_continuous_aggregate('conditions_summary_weekly', '2021-06-28', '2021-07-12');

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS week, min, max
FROM conditions_summary_weekly
ORDER BY week, city;

-- Make sure the invalidation threshold has changed
SELECT _timescaledb_internal.to_timestamp(watermark) at time zone 'UTC'
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Check if CREATE MATERIALIZED VIEW ... WITH DATA works.
-- Use monthly buckets this time and specify June 2000 as an origin.
CREATE MATERIALIZED VIEW conditions_summary_monthly
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('1 month', day, '2000-06-01' :: date) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket;

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_monthly
ORDER BY month, city;

-- Check the invalidation.
-- Step 1/2. Insert some more data , do a refresh and make sure that the
--           invalidation log is empty.
INSERT INTO conditions (day, city, temperature)
SELECT ts :: date, city, row_number() OVER ()
FROM generate_series('2021-09-01' :: date, '2021-09-15', '1 day') as ts,
     unnest(array['Moscow', 'Berlin']) as city;
CALL refresh_continuous_aggregate('conditions_summary_monthly', '2021-09-01', '2021-10-01');

SELECT
    _timescaledb_internal.to_timestamp(lowest_modified_value) AS lowest,
    _timescaledb_internal.to_timestamp(greatest_modified_value) AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_monthly
ORDER BY month, city;

-- Step 2/2. Add more data below the invalidation threshold, make sure that the
--           invalidation log is not empty, then do a refresh.
INSERT INTO conditions (day, city, temperature)
SELECT ts :: date, city, (CASE WHEN city = 'Moscow' THEN -40 ELSE 40 END)
FROM generate_series('2021-09-16' :: date, '2021-09-30', '1 day') as ts,
     unnest(array['Moscow', 'Berlin']) as city;

SELECT
    _timescaledb_internal.to_timestamp(lowest_modified_value) at time zone 'UTC' AS lowest,
    _timescaledb_internal.to_timestamp(greatest_modified_value) at time zone 'UTC' AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

CALL refresh_continuous_aggregate('conditions_summary_monthly', '2021-09-01', '2021-10-01');

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_monthly
ORDER BY month, city;

SELECT
    _timescaledb_internal.to_timestamp(lowest_modified_value) AS lowest,
    _timescaledb_internal.to_timestamp(greatest_modified_value) AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

-- Create a real-time aggregate with custom origin - June 2000
CREATE MATERIALIZED VIEW conditions_summary_rt
WITH (timescaledb.continuous) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day, '2000-06-01' :: date) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions
GROUP BY city, bucket;

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_rt
ORDER BY month, city;

-- Add some data to the hypertable and make sure it is visible in the cagg
INSERT INTO conditions (day, city, temperature) VALUES
  ('2021-10-01', 'Moscow', 1),
  ('2021-10-02', 'Moscow', 2),
  ('2021-10-03', 'Moscow', 3),
  ('2021-10-04', 'Moscow', 4),
  ('2021-10-01', 'Berlin', 5),
  ('2021-10-02', 'Berlin', 6),
  ('2021-10-03', 'Berlin', 7),
  ('2021-10-04', 'Berlin', 8);

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_rt
ORDER BY month, city;

-- Refresh the cagg and make sure that the result of SELECT query didn't change
CALL refresh_continuous_aggregate('conditions_summary_rt', '2021-10-01', '2021-11-01');

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_rt
ORDER BY month, city;

-- Add some more data, enable compression, compress the chunks and repeat the test

INSERT INTO conditions (day, city, temperature) VALUES
  ('2021-11-01', 'Moscow', 11),
  ('2021-11-02', 'Moscow', 12),
  ('2021-11-03', 'Moscow', 13),
  ('2021-11-04', 'Moscow', 14),
  ('2021-11-01', 'Berlin', 15),
  ('2021-11-02', 'Berlin', 16),
  ('2021-11-03', 'Berlin', 17),
  ('2021-11-04', 'Berlin', 18);

ALTER TABLE conditions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'city'
);

SELECT compress_chunk(ch) FROM show_chunks('conditions') AS ch;

-- Data for 2021-11 is seen because the cagg is real-time
SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_rt
ORDER BY month, city;

CALL refresh_continuous_aggregate('conditions_summary_rt', '2021-11-01', '2021-12-01');

-- Data for 2021-11 is seen because the cagg was refreshed
SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_rt
ORDER BY month, city;

-- Clean up
DROP TABLE conditions CASCADE;

-- Test caggs with monthly buckets and custom origin on top of distributed hypertable
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3

SELECT (add_data_node (name, host => 'localhost', DATABASE => name)).*
FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v (name);

GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO PUBLIC;

SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE TABLE conditions_dist(
  day date NOT NULL,
  temperature INT NOT NULL);

SELECT table_name FROM create_distributed_hypertable('conditions_dist', 'day', chunk_time_interval => INTERVAL '1 day');

INSERT INTO conditions_dist(day, temperature)
SELECT ts, date_part('month', ts)*100 + date_part('day', ts)
FROM generate_series('2010-01-01' :: date, '2010-03-01' :: date - interval '1 day', '1 day') as ts;

CREATE MATERIALIZED VIEW conditions_dist_1m
WITH (timescaledb.continuous) AS
SELECT
   timescaledb_experimental.time_bucket_ng('1 month', day, '2010-01-01') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_dist
GROUP BY bucket;

SELECT mat_hypertable_id AS cagg_id, raw_hypertable_id AS ht_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'conditions_dist_1m'
\gset

SELECT bucket_width
FROM _timescaledb_catalog.continuous_agg
WHERE mat_hypertable_id = :cagg_id;

SELECT experimental, name, bucket_width, origin, timezone
FROM _timescaledb_catalog.continuous_aggs_bucket_function
WHERE mat_hypertable_id = :cagg_id;

SELECT * FROM conditions_dist_1m ORDER BY bucket;

-- Same test but with non-realtime, NO DATA aggregate and manual refresh

CREATE MATERIALIZED VIEW conditions_dist_1m_manual
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
   timescaledb_experimental.time_bucket_ng('1 month', day, '2005-01-01') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_dist
GROUP BY bucket
WITH NO DATA;

SELECT * FROM conditions_dist_1m_manual ORDER BY bucket;

CALL refresh_continuous_aggregate('conditions_dist_1m_manual', '2010-01-01', '2010-03-01');
SELECT * FROM conditions_dist_1m_manual ORDER BY bucket;

-- Check invalidation for caggs on top of distributed hypertable

INSERT INTO conditions_dist(day, temperature)
VALUES ('2010-01-15', 999), ('2010-02-15', -999), ('2010-03-01', 15);

SELECT * FROM conditions_dist_1m ORDER BY bucket;
SELECT * FROM conditions_dist_1m_manual ORDER BY bucket;

CALL refresh_continuous_aggregate('conditions_dist_1m', '2010-01-01', '2010-04-01');
SELECT * FROM conditions_dist_1m ORDER BY bucket;
SELECT * FROM conditions_dist_1m_manual ORDER BY bucket;

CALL refresh_continuous_aggregate('conditions_dist_1m_manual', '2010-01-01', '2010-04-01');
SELECT * FROM conditions_dist_1m ORDER BY bucket;
SELECT * FROM conditions_dist_1m_manual ORDER BY bucket;

-- Compression on top of distributed hypertables

ALTER MATERIALIZED VIEW conditions_dist_1m_manual SET ( timescaledb.compress );

SELECT compress_chunk(ch)
FROM show_chunks('conditions_dist_1m_manual') ch limit 1;

SELECT * FROM conditions_dist_1m_manual ORDER BY bucket;

-- Clean up
DROP TABLE conditions_dist CASCADE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SELECT delete_data_node(name)
FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v (name);
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Test the specific code path of creating a CAGG on top of empty hypertable.

CREATE TABLE conditions_empty(
  day DATE NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions_empty', 'day',
  chunk_time_interval => INTERVAL '1 day'
);

CREATE MATERIALIZED VIEW conditions_summary_empty
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day, '2005-02-01') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_empty
GROUP BY city, bucket;

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_empty
ORDER BY month, city;

-- The test above changes the record that gets added to the invalidation log
-- for an empty table. Make sure it doesn't have any unintended side-effects
-- and the refreshing works as expected.

INSERT INTO conditions_empty (day, city, temperature) VALUES
  ('2021-06-14', 'Moscow', 26),
  ('2021-06-15', 'Moscow', 22),
  ('2021-06-16', 'Moscow', 24),
  ('2021-06-17', 'Moscow', 24),
  ('2021-06-18', 'Moscow', 27),
  ('2021-06-19', 'Moscow', 28),
  ('2021-06-20', 'Moscow', 30),
  ('2021-06-21', 'Moscow', 31),
  ('2021-06-22', 'Moscow', 34),
  ('2021-06-23', 'Moscow', 34),
  ('2021-06-24', 'Moscow', 34),
  ('2021-06-25', 'Moscow', 32),
  ('2021-06-26', 'Moscow', 32),
  ('2021-06-27', 'Moscow', 31);

CALL refresh_continuous_aggregate('conditions_summary_empty', '2021-06-01', '2021-07-01');

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_empty
ORDER BY month, city;

-- Clean up
DROP TABLE conditions_empty CASCADE;

-- Make sure add_continuous_aggregate_policy() works

CREATE TABLE conditions_policy(
  day DATE NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions_policy', 'day',
  chunk_time_interval => INTERVAL '1 day'
);

INSERT INTO conditions_policy (day, city, temperature) VALUES
  ('2021-06-14', 'Moscow', 26),
  ('2021-06-15', 'Moscow', 22),
  ('2021-06-16', 'Moscow', 24),
  ('2021-06-17', 'Moscow', 24),
  ('2021-06-18', 'Moscow', 27),
  ('2021-06-19', 'Moscow', 28),
  ('2021-06-20', 'Moscow', 30),
  ('2021-06-21', 'Moscow', 31),
  ('2021-06-22', 'Moscow', 34),
  ('2021-06-23', 'Moscow', 34),
  ('2021-06-24', 'Moscow', 34),
  ('2021-06-25', 'Moscow', 32),
  ('2021-06-26', 'Moscow', 32),
  ('2021-06-27', 'Moscow', 31);

CREATE MATERIALIZED VIEW conditions_summary_policy
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day, '2005-03-01') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_policy
GROUP BY city, bucket;

SELECT * FROM conditions_summary_policy;

\set ON_ERROR_STOP 0
-- Check for "policy refresh window too small" error
SELECT add_continuous_aggregate_policy('conditions_summary_policy',
    -- Historically, 1 month is just a synonym to 30 days here.
    -- See interval_to_int64() and interval_to_int128().
    start_offset => INTERVAL '2 months', 
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour');
\set ON_ERROR_STOP 1

SELECT add_continuous_aggregate_policy('conditions_summary_policy',
    start_offset => INTERVAL '65 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour');

-- Clean up
DROP TABLE conditions_policy CASCADE;

-- Make sure CAGGs with custom origin work for timestamp type

CREATE TABLE conditions_timestamp(
  tstamp TIMESTAMP NOT NULL,
  city TEXT NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions_timestamp', 'tstamp',
  chunk_time_interval => INTERVAL '1 day'
);

CREATE MATERIALIZED VIEW conditions_summary_timestamp
WITH (timescaledb.continuous) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('12 hours', tstamp, '2000-06-01 12:00:00') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_timestamp
GROUP BY city, bucket;

SELECT city, to_char(bucket, 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamp
ORDER BY b, city;

-- Add some data to the hypertable and make sure it is visible in the cagg
INSERT INTO conditions_timestamp(tstamp, city, temperature)
SELECT ts, city, (CASE WHEN city = 'Moscow' THEN 20000 ELSE 10000 END) + date_part('day', ts)*100 + date_part('hour', ts)
FROM
  generate_series('2010-01-01 00:00:00' :: timestamp, '2010-01-02 00:00:00' :: timestamp - interval '1 hour', '1 hour') as ts,
  unnest(array['Moscow', 'Berlin']) as city;

SELECT city, to_char(bucket, 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamp
ORDER BY b, city;

-- Refresh the cagg and make sure that the result of SELECT query didn't change
CALL refresh_continuous_aggregate('conditions_summary_timestamp', '2010-01-01 00:00:00', '2010-01-02 00:00:00');

SELECT city, to_char(bucket, 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamp
ORDER BY b, city;

-- Add some more data, enable compression, compress the chunks and repeat the test

INSERT INTO conditions_timestamp(tstamp, city, temperature)
SELECT ts, city, (CASE WHEN city = 'Moscow' THEN 20000 ELSE 10000 END) + date_part('day', ts)*100 + date_part('hour', ts)
FROM
  generate_series('2010-01-02 00:00:00' :: timestamp, '2010-01-03 00:00:00' :: timestamp - interval '1 hour', '1 hour') as ts,
  unnest(array['Moscow', 'Berlin']) as city;

ALTER TABLE conditions_timestamp SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'city'
);

SELECT compress_chunk(ch) FROM show_chunks('conditions_timestamp') AS ch;

-- New data is seen because the cagg is real-time
SELECT city, to_char(bucket, 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamp
ORDER BY b, city;

CALL refresh_continuous_aggregate('conditions_summary_timestamp', '2010-01-02 00:00:00', '2010-01-03 00:00:00');

-- New data is seen because the cagg was refreshed
SELECT city, to_char(bucket, 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamp
ORDER BY b, city;

-- Add a refresh policy
SELECT add_continuous_aggregate_policy('conditions_summary_timestamp',
    start_offset => INTERVAL '25 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes');

-- Clean up
DROP TABLE conditions_timestamp CASCADE;

-- Make sure CAGGs with custom origin work for timestamptz type

CREATE TABLE conditions_timestamptz(
  tstamp TIMESTAMPTZ NOT NULL,
  city TEXT NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions_timestamptz', 'tstamp',
  chunk_time_interval => INTERVAL '1 day'
);

\set ON_ERROR_STOP 0

-- For monthly buckets origin should be the first day of the month in given timezone
-- 2020-06-02 00:00:00 MSK == 2020-06-01 21:00:00 UTC
CREATE MATERIALIZED VIEW conditions_summary_timestamptz
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('1 month', tstamp, '2020-06-02 00:00:00 MSK', 'Europe/Moscow') AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions_timestamptz
GROUP BY city, bucket;

-- Make sure buckets like '1 months 15 days" (fixed+variable-sized) are not allowed
CREATE MATERIALIZED VIEW conditions_summary_timestamptz
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('1 month 15 days', tstamp, '2020-06-01 00:00:00 MSK', 'Europe/Moscow') AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions_timestamptz
GROUP BY city, bucket;

\set ON_ERROR_STOP 1

CREATE MATERIALIZED VIEW conditions_summary_timestamptz
WITH (timescaledb.continuous) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('12 hours', tstamp, '2020-06-01 12:00:00 MSK', 'Europe/Moscow') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_timestamptz
GROUP BY city, bucket;

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamptz
ORDER BY b, city;

-- Add some data to the hypertable and make sure it is visible in the cagg
INSERT INTO conditions_timestamptz(tstamp, city, temperature)
SELECT ts, city,
  (CASE WHEN city = 'Moscow' THEN 20000 ELSE 10000 END) +
  date_part('day', ts at time zone 'MSK')*100 +
  date_part('hour', ts at time zone 'MSK')
FROM
  generate_series('2022-01-01 00:00:00 MSK' :: timestamptz, '2022-01-02 00:00:00 MSK' :: timestamptz - interval '1 hour', '1 hour') as ts,
  unnest(array['Moscow', 'Berlin']) as city;

-- Check the data
SELECT to_char(tstamp at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS ts, city, temperature FROM conditions_timestamptz
ORDER BY ts, city;

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamptz
ORDER BY b, city;

-- Refresh the cagg and make sure that the result of SELECT query didn't change
CALL refresh_continuous_aggregate('conditions_summary_timestamptz', '2022-01-01 00:00:00 MSK', '2022-01-02 00:00:00 MSK');

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamptz
ORDER BY b, city;

-- Add some more data, enable compression, compress the chunks and repeat the test

INSERT INTO conditions_timestamptz(tstamp, city, temperature)
SELECT ts, city,
  (CASE WHEN city = 'Moscow' THEN 20000 ELSE 10000 END) +
  date_part('day', ts at time zone 'MSK')*100 +
  date_part('hour', ts at time zone 'MSK')
FROM
  generate_series('2022-01-02 00:00:00 MSK' :: timestamptz, '2022-01-03 00:00:00 MSK' :: timestamptz - interval '1 hour', '1 hour') as ts,
  unnest(array['Moscow', 'Berlin']) as city;

ALTER TABLE conditions_timestamptz SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'city'
);

SELECT compress_chunk(ch) FROM show_chunks('conditions_timestamptz') AS ch;

-- New data is seen because the cagg is real-time
SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamptz
ORDER BY b, city;

CALL refresh_continuous_aggregate('conditions_summary_timestamptz', '2022-01-02 00:00:00 MSK', '2022-01-03 00:00:00 MSK');

-- New data is seen because the cagg was refreshed
SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS b, min, max
FROM conditions_summary_timestamptz
ORDER BY b, city;

-- Add a refresh policy
SELECT add_continuous_aggregate_policy('conditions_summary_timestamptz',
    start_offset => INTERVAL '25 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '30 minutes');

-- Clean up
DROP TABLE conditions_timestamptz CASCADE;
