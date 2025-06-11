-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Make sure that timezone can't be used for types other that timestamptz

CREATE TABLE conditions(
  day timestamp NOT NULL, -- not timestamptz!
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions', 'day',
  chunk_time_interval => INTERVAL '1 day'
);

\set ON_ERROR_STOP 0
-- timebucket_ng is deprecated and can not be used in new CAggs anymore.
-- However, using this GUC the restriction can be lifted in debug builds
-- to ensure the functionality can be tested.
SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;

CREATE MATERIALIZED VIEW conditions
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day, 'Europe/Moscow') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;

-- Reset GUC to check if the CAgg would also work in release builds
RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;

\set ON_ERROR_STOP 1

DROP TABLE conditions CASCADE;

CREATE TABLE conditions_tz(
  day timestamptz NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions_tz', 'day',
  chunk_time_interval => INTERVAL '1 day'
);

INSERT INTO conditions_tz (day, city, temperature) VALUES
  ('2021-06-14 00:00:00 MSK', 'Moscow', 26),
  ('2021-06-15 00:00:00 MSK', 'Moscow', 22),
  ('2021-06-16 00:00:00 MSK', 'Moscow', 24),
  ('2021-06-17 00:00:00 MSK', 'Moscow', 24),
  ('2021-06-18 00:00:00 MSK', 'Moscow', 27),
  ('2021-06-19 00:00:00 MSK', 'Moscow', 28),
  ('2021-06-20 00:00:00 MSK', 'Moscow', 30),
  ('2021-06-21 00:00:00 MSK', 'Moscow', 31),
  ('2021-06-22 00:00:00 MSK', 'Moscow', 34),
  ('2021-06-23 00:00:00 MSK', 'Moscow', 34),
  ('2021-06-24 00:00:00 MSK', 'Moscow', 34),
  ('2021-06-25 00:00:00 MSK', 'Moscow', 32),
  ('2021-06-26 00:00:00 MSK', 'Moscow', 32),
  ('2021-06-27 00:00:00 MSK', 'Moscow', 31);

\set ON_ERROR_STOP 0

-- Check that the name of the timezone is validated
SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;

CREATE MATERIALIZED VIEW conditions_summary_tz
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day, 'Europe/Ololondon') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_tz
GROUP BY city, bucket
WITH NO DATA;

-- Check that buckets like '1 month 15 days' (fixed-sized + variable-sized) are not allowed

CREATE MATERIALIZED VIEW conditions_summary_tz
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month 15 days', day, 'MSK') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_tz
GROUP BY city, bucket
WITH NO DATA;

-- Check that only immutable expressions can be used as a timezone

CREATE MATERIALIZED VIEW conditions_summary_tz
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 day', day, city) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_tz
GROUP BY city, bucket
WITH NO DATA;

\set ON_ERROR_STOP 1

-- Make sure it's possible to create an empty cagg (WITH NO DATA) and
-- that all the information about the bucketing function will be saved
-- to the TS catalog.

CREATE MATERIALIZED VIEW conditions_summary_tz
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day, 'MSK') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_tz
GROUP BY city, bucket
WITH NO DATA;

RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;

SELECT mat_hypertable_id AS cagg_id_tz, raw_hypertable_id AS ht_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'conditions_summary_tz'
\gset

-- Make sure the timezone is saved in the catalog table
SELECT bf.*
FROM _timescaledb_catalog.continuous_agg, LATERAL _timescaledb_functions.cagg_get_bucket_function_info(mat_hypertable_id) AS bf
WHERE mat_hypertable_id = :cagg_id_tz;

-- Make sure that buckets with specified timezone are always treated as
-- variable-sized, even if the interval is fixed (i.e. days and/or hours)

SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;

CREATE MATERIALIZED VIEW conditions_summary_1w
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('7 days', day, 'MSK') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_tz
GROUP BY city, bucket
WITH NO DATA;

RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;

SELECT mat_hypertable_id AS cagg_id_1w
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'conditions_summary_1w'
\gset

-- Make sure the timezone is saved in the catalog table
SELECT bf.*
FROM _timescaledb_catalog.continuous_agg, LATERAL _timescaledb_functions.cagg_get_bucket_function_info(mat_hypertable_id) AS bf
WHERE mat_hypertable_id = :cagg_id_1w;

-- Check the invalidation threshold is -infinity
SELECT _timescaledb_functions.to_timestamp(watermark) at time zone 'MSK'
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Make sure the invalidation log is empty
SELECT
    to_char(_timescaledb_functions.to_timestamp(lowest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS lowest,
    to_char(_timescaledb_functions.to_timestamp(greatest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

-- Make sure truncating of the refresh window works
\set ON_ERROR_STOP 0
CALL refresh_continuous_aggregate('conditions_summary_tz', '2021-07-02 MSK', '2021-07-12 MSK');
CALL refresh_continuous_aggregate('conditions_summary_1w', '2021-07-02 MSK', '2021-07-05 MSK');
\set ON_ERROR_STOP 1

-- Make sure refreshing works
CALL refresh_continuous_aggregate('conditions_summary_tz', '2021-06-01 MSK', '2021-07-01 MSK');

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_tz
ORDER by month, city;

-- Check the invalidation threshold
SELECT to_char(_timescaledb_functions.to_timestamp(watermark) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS')
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- The default origin is Saturday. Here we do refresh only for two full weeks
-- in June in order to keep the invalidation threshold as it is.
CALL refresh_continuous_aggregate('conditions_summary_1w', '2021-06-12 MSK', '2021-06-26 MSK');

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as week, min, max
FROM conditions_summary_1w
ORDER by week, city;

-- Check the invalidation threshold

SELECT to_char(_timescaledb_functions.to_timestamp(watermark) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS')
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Make sure creating CAGGs without NO DATA works the same way

SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;
CREATE MATERIALIZED VIEW conditions_summary_tz2
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day, 'MSK') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_tz
GROUP BY city, bucket;
RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_tz2
ORDER by month, city;

-- Add some dummy data for two more months (no invalidations test case)
INSERT INTO conditions_tz (day, city, temperature)
SELECT ts, city, row_number() OVER ()
FROM generate_series('2021-07-01 MSK' :: timestamptz, '2021-08-31 MSK', '3 day') as ts,
     unnest(array['Moscow', 'Berlin']) as city;

-- Double check the generated data
SELECT to_char(day at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS'), city, temperature
FROM conditions_tz
WHERE day >= '2021-07-01 MSK'
ORDER BY city DESC, day;

-- Make sure the invalidation threshold was unaffected
SELECT to_char(_timescaledb_functions.to_timestamp(watermark) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS')
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Make sure the invalidation log is still empty
SELECT
    to_char(_timescaledb_functions.to_timestamp(lowest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS lowest,
    to_char(_timescaledb_functions.to_timestamp(greatest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

-- Call refresh for two full buckets: 2021-07-01 and 2021-08-01
CALL refresh_continuous_aggregate('conditions_summary_tz', '2021-06-15 MSK', '2021-09-15 MSK');

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_tz
ORDER by month, city;

-- Make sure the invalidation threshold has changed
SELECT to_char(_timescaledb_functions.to_timestamp(watermark) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS')
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Make sure the invalidation log is still empty
SELECT
    to_char(_timescaledb_functions.to_timestamp(lowest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS lowest,
    to_char(_timescaledb_functions.to_timestamp(greatest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

-- Add more data below the invalidation threshold, make sure that the
-- invalidation log is not empty, then do a refresh.
INSERT INTO conditions_tz (day, city, temperature)
SELECT ts :: timestamptz, city, (CASE WHEN city = 'Moscow' THEN -100 ELSE 100 END)
FROM generate_series('2021-08-16 MSK' :: timestamptz, '2021-08-30 MSK', '1 day') as ts,
     unnest(array['Moscow', 'Berlin']) as city;

SELECT
    to_char(_timescaledb_functions.to_timestamp(lowest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS lowest,
    to_char(_timescaledb_functions.to_timestamp(greatest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

CALL refresh_continuous_aggregate('conditions_summary_tz', '2021-08-01 MSK', '2021-09-01 MSK');

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_tz
ORDER by month, city;

SELECT
    to_char(_timescaledb_functions.to_timestamp(lowest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS lowest,
    to_char(_timescaledb_functions.to_timestamp(greatest_modified_value) at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

-- Clean up
DROP MATERIALIZED VIEW conditions_summary_tz;
DROP MATERIALIZED VIEW conditions_summary_1w;

-- Create a real-time aggregate
SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;
CREATE MATERIALIZED VIEW conditions_summary_tz
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day, 'MSK') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_tz
GROUP BY city, bucket;
RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_tz
ORDER by month, city;

-- Add some data to the hypertable and make sure they are visible in the cagg
INSERT INTO conditions_tz (day, city, temperature) VALUES
  ('2021-10-01 00:00:00 MSK', 'Moscow', 1),
  ('2021-10-02 00:00:00 MSK', 'Moscow', 2),
  ('2021-10-03 00:00:00 MSK', 'Moscow', 3),
  ('2021-10-04 00:00:00 MSK', 'Moscow', 4),
  ('2021-10-01 00:00:00 MSK', 'Berlin', 5),
  ('2021-10-02 00:00:00 MSK', 'Berlin', 6),
  ('2021-10-03 00:00:00 MSK', 'Berlin', 7),
  ('2021-10-04 00:00:00 MSK', 'Berlin', 8);

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_tz
ORDER by month, city;

-- Refresh the cagg and make sure that the result of SELECT query didn't change
CALL refresh_continuous_aggregate('conditions_summary_tz', '2021-10-01 00:00:00 MSK', '2021-11-01 00:00:00 MSK');

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_tz
ORDER by month, city;

-- Add some more data, enable compression, compress the chunks and repeat the test

INSERT INTO conditions_tz (day, city, temperature) VALUES
  ('2021-11-01 00:00:00 MSK', 'Moscow', 11),
  ('2021-11-02 00:00:00 MSK', 'Moscow', 12),
  ('2021-11-03 00:00:00 MSK', 'Moscow', 13),
  ('2021-11-04 00:00:00 MSK', 'Moscow', 14),
  ('2021-11-01 00:00:00 MSK', 'Berlin', 15),
  ('2021-11-02 00:00:00 MSK', 'Berlin', 16),
  ('2021-11-03 00:00:00 MSK', 'Berlin', 17),
  ('2021-11-04 00:00:00 MSK', 'Berlin', 18);

ALTER TABLE conditions_tz SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'city'
);

SELECT compress_chunk(ch) FROM show_chunks('conditions_tz') AS ch;

-- Data for 2021-11 is seen because the cagg is real-time
SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_tz
ORDER by month, city;

CALL refresh_continuous_aggregate('conditions_summary_tz', '2021-11-01 00:00:00 MSK', '2021-12-01 00:00:00 MSK');

-- Data for 2021-11 is seen because the cagg was refreshed
SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_tz
ORDER by month, city;

-- Test for some more cases: single CAGG per HT, creating CAGG on top of an
-- empty HT, buckets other than 1 month.

CREATE TABLE conditions2(
  day timestamptz NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions2', 'day',
  chunk_time_interval => INTERVAL '1 day'
);

-- Create a real-time aggregate on top of empty HT
SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;
CREATE MATERIALIZED VIEW conditions2_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('7 days', day, 'MSK') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions2
GROUP BY city, bucket;
RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;

INSERT INTO conditions2 (day, city, temperature) VALUES
  ('2021-06-14 00:00:00 MSK', 'Moscow', 26),
  ('2021-06-15 00:00:00 MSK', 'Moscow', 22),
  ('2021-06-16 00:00:00 MSK', 'Moscow', 24),
  ('2021-06-17 00:00:00 MSK', 'Moscow', 24),
  ('2021-06-18 00:00:00 MSK', 'Moscow', 27),
  ('2021-06-19 00:00:00 MSK', 'Moscow', 28),
  ('2021-06-20 00:00:00 MSK', 'Moscow', 30),
  ('2021-06-21 00:00:00 MSK', 'Moscow', 31),
  ('2021-06-22 00:00:00 MSK', 'Moscow', 34),
  ('2021-06-23 00:00:00 MSK', 'Moscow', 34),
  ('2021-06-24 00:00:00 MSK', 'Moscow', 34),
  ('2021-06-25 00:00:00 MSK', 'Moscow', 32),
  ('2021-06-26 00:00:00 MSK', 'Moscow', 32),
  ('2021-06-27 00:00:00 MSK', 'Moscow', 31);

-- All data should be seen for a real-time aggregate
SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions2_summary
ORDER by month, city;

-- Refresh should work
CALL refresh_continuous_aggregate('conditions2_summary', '2021-06-12 MSK', '2021-07-03 MSK');

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions2_summary
ORDER by month, city;

-- New data should be seen
INSERT INTO conditions2 (day, city, temperature) VALUES
  ('2021-09-30 00:00:00 MSK', 'Moscow', 0),
  ('2021-10-01 00:00:00 MSK', 'Moscow', 1),
  ('2021-10-02 00:00:00 MSK', 'Moscow', 2),
  ('2021-10-03 00:00:00 MSK', 'Moscow', 3),
  ('2021-10-04 00:00:00 MSK', 'Moscow', 4),
  ('2021-09-30 00:00:00 MSK', 'Berlin', 5),
  ('2021-10-01 00:00:00 MSK', 'Berlin', 6),
  ('2021-10-02 00:00:00 MSK', 'Berlin', 7),
  ('2021-10-03 00:00:00 MSK', 'Berlin', 8),
  ('2021-10-04 00:00:00 MSK', 'Berlin', 9);

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions2_summary
ORDER by month, city;

-- Make sure add_continuous_aggregate_policy() works

CREATE TABLE conditions_policy(
  day TIMESTAMPTZ NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions_policy', 'day',
  chunk_time_interval => INTERVAL '1 day'
);

INSERT INTO conditions_policy (day, city, temperature) VALUES
  ('2021-06-14 00:00:00 MSK', 'Moscow', 26),
  ('2021-06-14 10:00:00 MSK', 'Moscow', 22),
  ('2021-06-14 20:00:00 MSK', 'Moscow', 24),
  ('2021-06-15 00:00:00 MSK', 'Moscow', 24),
  ('2021-06-15 10:00:00 MSK', 'Moscow', 27),
  ('2021-06-15 20:00:00 MSK', 'Moscow', 28),
  ('2021-06-16 00:00:00 MSK', 'Moscow', 30),
  ('2021-06-16 10:00:00 MSK', 'Moscow', 31),
  ('2021-06-16 20:00:00 MSK', 'Moscow', 34),
  ('2021-06-17 00:00:00 MSK', 'Moscow', 34),
  ('2021-06-17 10:00:00 MSK', 'Moscow', 34),
  ('2021-06-17 20:00:00 MSK', 'Moscow', 32),
  ('2021-06-18 00:00:00 MSK', 'Moscow', 32),
  ('2021-06-18 10:00:00 MSK', 'Moscow', 31),
  ('2021-06-18 20:00:00 MSK', 'Moscow', 26);

SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;
CREATE MATERIALIZED VIEW conditions_summary_policy
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 day', day, 'Europe/Moscow') AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_policy
GROUP BY city, bucket;
RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;

SELECT city, to_char(bucket at time zone 'MSK', 'YYYY-MM-DD HH24:MI:SS') as month, min, max
FROM conditions_summary_policy
ORDER by month, city;

\set ON_ERROR_STOP 0
-- Check for "policy refresh window too small" error
SELECT add_continuous_aggregate_policy('conditions_summary_policy',
    start_offset => INTERVAL '2 days 23 hours',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour');
\set ON_ERROR_STOP 1

SELECT add_continuous_aggregate_policy('conditions_summary_policy',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 hour');
