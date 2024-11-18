-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Make sure experimental immutable function with 2 arguments can be used in caggs.
-- Functions with 3 arguments and/or stable functions are currently not supported in caggs.

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

-- timebucket_ng is deprecated and can not be used in new CAggs anymore.
-- However, using this GUC the restriction can be lifted in debug builds
-- to ensure the functionality can be tested.
SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;

CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('7 days', day) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket;

-- Reset GUC to check if the CAgg would also work in release builds
RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;

SELECT to_char(bucket, 'YYYY-MM-DD'), city, min, max
FROM conditions_summary_weekly
ORDER BY bucket;

DROP TABLE conditions CASCADE;

-- Make sure seconds, minutes, and hours can be used with caggs ('origin' is not
-- currently supported in caggs).
CREATE TABLE conditions(
  tstamp TIMESTAMP NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions', 'tstamp',
  chunk_time_interval => INTERVAL '1 day'
);

INSERT INTO conditions (tstamp, city, temperature) VALUES
  ('2021-06-14 12:30:00', 'Moscow', 26),
  ('2021-06-14 12:30:10', 'Moscow', 22),
  ('2021-06-14 12:30:20', 'Moscow', 24),
  ('2021-06-14 12:30:30', 'Moscow', 24),
  ('2021-06-14 12:30:40', 'Moscow', 27),
  ('2021-06-14 12:30:50', 'Moscow', 28),
  ('2021-06-14 12:31:10', 'Moscow', 30),
  ('2021-06-14 12:31:20', 'Moscow', 31),
  ('2021-06-14 12:31:30', 'Moscow', 34),
  ('2021-06-14 12:31:40', 'Moscow', 34),
  ('2021-06-14 12:31:50', 'Moscow', 34),
  ('2021-06-14 12:32:00', 'Moscow', 32),
  ('2021-06-14 12:32:10', 'Moscow', 32),
  ('2021-06-14 12:32:20', 'Moscow', 31);

SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;
CREATE MATERIALIZED VIEW conditions_summary_30sec
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('30 seconds', tstamp) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket;

CREATE MATERIALIZED VIEW conditions_summary_1min
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('1 minute', tstamp) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket;

CREATE MATERIALIZED VIEW conditions_summary_1hour
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('1 hour', tstamp) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket;
RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;


SELECT city, to_char(bucket, 'YYYY-MM-DD HH:mi:ss'), min, max FROM conditions_summary_30sec ORDER BY bucket;
SELECT city, to_char(bucket, 'YYYY-MM-DD HH:mi:ss'), min, max FROM conditions_summary_1min ORDER BY bucket;
SELECT city, to_char(bucket, 'YYYY-MM-DD HH:mi:ss'), min, max FROM conditions_summary_1hour ORDER BY bucket;

DROP TABLE conditions CASCADE;

-- Experimental functions using different schema for installation than PUBLIC
\c :TEST_DBNAME :ROLE_SUPERUSER

\set TEST_DBNAME_2 :TEST_DBNAME _2
CREATE DATABASE :TEST_DBNAME_2;
\c :TEST_DBNAME_2 :ROLE_SUPERUSER
CREATE SCHEMA test1;
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb SCHEMA test1;

CREATE TABLE conditions(
  tstamp TIMESTAMP NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT test1.create_hypertable(
  'conditions', 'tstamp',
  chunk_time_interval => INTERVAL '1 day'
);

SET timescaledb.debug_allow_cagg_with_deprecated_funcs = true;
CREATE MATERIALIZED VIEW conditions_summary_monthly
WITH (timescaledb.continuous) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('1 month', tstamp) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;
RESET timescaledb.debug_allow_cagg_with_deprecated_funcs;

CREATE MATERIALIZED VIEW conditions_summary_yearly
WITH (timescaledb.continuous) AS
SELECT city,
       test1.time_bucket('1 year', tstamp) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;

SELECT bucket_func, bucket_width, bucket_origin, bucket_timezone, bucket_fixed_width
FROM _timescaledb_catalog.continuous_aggs_bucket_function ORDER BY 1;

-- Try to toggle realtime feature on existing CAgg using timescaledb_experimental.time_bucket_ng
ALTER MATERIALIZED VIEW conditions_summary_monthly SET (timescaledb.materialized_only=false);
ALTER MATERIALIZED VIEW conditions_summary_monthly SET (timescaledb.materialized_only=true);

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE :TEST_DBNAME_2 WITH (FORCE);
