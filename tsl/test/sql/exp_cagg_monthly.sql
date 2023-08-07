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

-- Check that buckets like '1 month 15 days' (fixed-sized + variable-sized) are not allowed

\set ON_ERROR_STOP 0
CREATE MATERIALIZED VIEW conditions_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month 15 days', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;
\set ON_ERROR_STOP 1

-- Make sure it's possible to create an empty cagg (WITH NO DATA) and
-- that all the information about the bucketing function will be saved
-- to the TS catalog.

CREATE MATERIALIZED VIEW conditions_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions
GROUP BY city, bucket
WITH NO DATA;

SELECT mat_hypertable_id AS cagg_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'conditions_summary'
\gset

SELECT raw_hypertable_id AS ht_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'conditions_summary'
\gset

SELECT bucket_width
FROM _timescaledb_catalog.continuous_agg
WHERE mat_hypertable_id = :cagg_id;

SELECT experimental, name, bucket_width, origin, timezone
FROM _timescaledb_catalog.continuous_aggs_bucket_function
WHERE mat_hypertable_id = :cagg_id;

-- Check that the saved invalidation threshold is -infinity
SELECT _timescaledb_functions.to_timestamp(watermark)
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Make sure truncating of the refresh window works
\set ON_ERROR_STOP 0
CALL refresh_continuous_aggregate('conditions_summary', '2021-07-02', '2021-07-12');
\set ON_ERROR_STOP 1

-- Make sure refreshing works
CALL refresh_continuous_aggregate('conditions_summary', '2021-06-01', '2021-07-01');
SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary
ORDER by month, city;

-- Make sure larger refresh window is fine too
CALL refresh_continuous_aggregate('conditions_summary', '2021-03-01', '2021-07-01');
SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary
ORDER by month, city;

-- Special check for "invalid or missing information about the bucketing
-- function" code path
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
CREATE TEMPORARY TABLE restore_table ( LIKE _timescaledb_catalog.continuous_aggs_bucket_function );
INSERT INTO restore_table SELECT * FROM  _timescaledb_catalog.continuous_aggs_bucket_function;
DELETE FROM _timescaledb_catalog.continuous_aggs_bucket_function;
\set ON_ERROR_STOP 0
-- should fail with "invalid or missing information..."
CALL refresh_continuous_aggregate('conditions_summary', '2021-06-01', '2021-07-01');
\set ON_ERROR_STOP 1
INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function SELECT * FROM restore_table;
DROP TABLE restore_table;
-- should execute successfully
CALL refresh_continuous_aggregate('conditions_summary', '2021-06-01', '2021-07-01');
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Check the invalidation threshold
SELECT _timescaledb_functions.to_timestamp(watermark) at time zone 'UTC'
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Add some dummy data for two more months and call refresh (no invalidations test case)
INSERT INTO conditions (day, city, temperature)
SELECT ts :: date, city, row_number() OVER ()
FROM generate_series('2021-07-01' :: date, '2021-08-31', '1 day') as ts,
     unnest(array['Moscow', 'Berlin']) as city;

-- Double check generated data
SELECT to_char(day, 'YYYY-MM-DD'), city, temperature
FROM conditions
WHERE day >= '2021-07-01'
ORDER BY city DESC, day;

-- Make sure the invalidation threshold was unaffected
SELECT _timescaledb_functions.to_timestamp(watermark) at time zone 'UTC'
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Make sure the invalidation log is empty
SELECT
    _timescaledb_functions.to_timestamp(lowest_modified_value) AS lowest,
    _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

-- Call refresh
CALL refresh_continuous_aggregate('conditions_summary', '2021-06-15', '2021-09-15');

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary
ORDER by month, city;

-- Make sure the invalidation threshold has changed
SELECT _timescaledb_functions.to_timestamp(watermark) at time zone 'UTC'
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold
WHERE hypertable_id = :ht_id;

-- Make sure the catalog is cleaned up when the cagg is dropped
DROP MATERIALIZED VIEW conditions_summary;

SELECT * FROM _timescaledb_catalog.continuous_agg
WHERE mat_hypertable_id = :cagg_id;

SELECT * FROM _timescaledb_catalog.continuous_aggs_bucket_function
WHERE mat_hypertable_id = :cagg_id;

-- Re-create cagg, this time WITH DATA
CREATE MATERIALIZED VIEW conditions_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions
GROUP BY city, bucket;

-- Make sure cagg was filled
SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary
ORDER by month, city;

-- Check the invalidation.
-- Step 1/2. Insert some more data , do a refresh and make sure that the
--           invalidation log is empty.
INSERT INTO conditions (day, city, temperature)
SELECT ts :: date, city, row_number() OVER ()
FROM generate_series('2021-09-01' :: date, '2021-09-15', '1 day') as ts,
     unnest(array['Moscow', 'Berlin']) as city;
CALL refresh_continuous_aggregate('conditions_summary', '2021-09-01', '2021-10-01');

SELECT
    _timescaledb_functions.to_timestamp(lowest_modified_value) AS lowest,
    _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary
ORDER by month, city;

-- Step 2/2. Add more data below the invalidation threshold, make sure that the
--           invalidation log is not empty, that do a refresh.
INSERT INTO conditions (day, city, temperature)
SELECT ts :: date, city, (CASE WHEN city = 'Moscow' THEN -40 ELSE 40 END)
FROM generate_series('2021-09-16' :: date, '2021-09-30', '1 day') as ts,
     unnest(array['Moscow', 'Berlin']) as city;

SELECT
    _timescaledb_functions.to_timestamp(lowest_modified_value) at time zone 'UTC' AS lowest,
    _timescaledb_functions.to_timestamp(greatest_modified_value) at time zone 'UTC' AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

CALL refresh_continuous_aggregate('conditions_summary', '2021-09-01', '2021-10-01');

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary
ORDER by month, city;

SELECT
    _timescaledb_functions.to_timestamp(lowest_modified_value) AS lowest,
    _timescaledb_functions.to_timestamp(greatest_modified_value) AS greatest
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :ht_id;

-- Create a real-time aggregate
DROP MATERIALIZED VIEW conditions_summary;
CREATE MATERIALIZED VIEW conditions_summary
WITH (timescaledb.continuous) AS
SELECT city,
   timescaledb_experimental.time_bucket_ng('1 month', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions
GROUP BY city, bucket;

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary
ORDER by month, city;

-- Add some data to the hypertable and make sure they are visible in the cagg
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
FROM conditions_summary
ORDER by month, city;

-- Refresh the cagg and make sure that the result of SELECT query didn't change
CALL refresh_continuous_aggregate('conditions_summary', '2021-10-01', '2021-11-01');

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary
ORDER by month, city;

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
FROM conditions_summary
ORDER by month, city;

CALL refresh_continuous_aggregate('conditions_summary', '2021-11-01', '2021-12-01');

-- Data for 2021-11 is seen because the cagg was refreshed
SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary
ORDER by month, city;

-- Test N-months buckets where N in 2,3,4,5,6,12,13 on a relatively large table
-- This also tests the case when a single hypertable has multiple caggs.

CREATE TABLE conditions_large(
  day DATE NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions_large', 'day',
  chunk_time_interval => INTERVAL '1 month'
);

INSERT INTO conditions_large(day, temperature)
SELECT ts, date_part('month', ts)*100 + date_part('day', ts)
FROM generate_series('2010-01-01' :: date, '2020-01-01' :: date - interval '1 day', '1 day') as ts;

CREATE MATERIALIZED VIEW conditions_large_2m
WITH (timescaledb.continuous) AS
SELECT
   timescaledb_experimental.time_bucket_ng('2 months', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_large
GROUP BY bucket;

SELECT * FROM conditions_large_2m ORDER BY bucket;

CREATE MATERIALIZED VIEW conditions_large_3m
WITH (timescaledb.continuous) AS
SELECT
   timescaledb_experimental.time_bucket_ng('3 months', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_large
GROUP BY bucket;

SELECT * FROM conditions_large_3m ORDER BY bucket;

CREATE MATERIALIZED VIEW conditions_large_4m
WITH (timescaledb.continuous) AS
SELECT
   timescaledb_experimental.time_bucket_ng('4 months', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_large
GROUP BY bucket;

SELECT * FROM conditions_large_4m ORDER BY bucket;

CREATE MATERIALIZED VIEW conditions_large_5m
WITH (timescaledb.continuous) AS
SELECT
   timescaledb_experimental.time_bucket_ng('5 months', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_large
GROUP BY bucket;

SELECT * FROM conditions_large_5m ORDER BY bucket;

CREATE MATERIALIZED VIEW conditions_large_6m
WITH (timescaledb.continuous) AS
SELECT
   timescaledb_experimental.time_bucket_ng('6 months', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_large
GROUP BY bucket;

SELECT * FROM conditions_large_6m ORDER BY bucket;

CREATE MATERIALIZED VIEW conditions_large_1y
WITH (timescaledb.continuous) AS
SELECT
   timescaledb_experimental.time_bucket_ng('1 year', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_large
GROUP BY bucket;

SELECT * FROM conditions_large_1y ORDER BY bucket;

CREATE MATERIALIZED VIEW conditions_large_1y1m
WITH (timescaledb.continuous) AS
SELECT
   timescaledb_experimental.time_bucket_ng('1 year 1 month', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_large
GROUP BY bucket;

SELECT * FROM conditions_large_1y1m ORDER BY bucket;

-- Trigger merged refresh to check corresponding code path as well
DROP MATERIALIZED VIEW conditions_large_1y;
SET timescaledb.materializations_per_refresh_window = 0;

CREATE MATERIALIZED VIEW conditions_large_1y
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT
   timescaledb_experimental.time_bucket_ng('1 year', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_large
GROUP BY bucket;

SELECT * FROM conditions_large_1y ORDER BY bucket;

INSERT INTO conditions_large(day, temperature)
SELECT ts, date_part('month', ts)*100 + date_part('day', ts)
FROM generate_series('2020-01-01' :: date, '2021-01-01' :: date - interval '1 day', '1 day') as ts;

CALL refresh_continuous_aggregate('conditions_large_1y', '2020-01-01', '2021-01-01');

SELECT * FROM conditions_large_1y ORDER BY bucket;

RESET timescaledb.materializations_per_refresh_window;

-- Test caggs with monthly buckets on top of distributed hypertable
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3

SELECT node_name, database, node_created, database_created, extension_created
FROM (
  SELECT (add_data_node(name, host => 'localhost', DATABASE => name)).*
  FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v(name)
) a;

GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO PUBLIC;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE TABLE conditions_dist(
  day DATE NOT NULL,
  temperature INT NOT NULL);

SELECT table_name FROM create_distributed_hypertable('conditions_dist', 'day', chunk_time_interval => INTERVAL '1 day');

INSERT INTO conditions_dist(day, temperature)
SELECT ts, date_part('month', ts)*100 + date_part('day', ts)
FROM generate_series('2010-01-01' :: date, '2010-03-01' :: date - interval '1 day', '1 day') as ts;

CREATE MATERIALIZED VIEW conditions_dist_1m
WITH (timescaledb.continuous) AS
SELECT
   timescaledb_experimental.time_bucket_ng('1 month', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_dist
GROUP BY bucket;

SELECT mat_hypertable_id AS cagg_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'conditions_dist_1m'
\gset

SELECT raw_hypertable_id AS ht_id
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
   timescaledb_experimental.time_bucket_ng('1 month', day) AS bucket,
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

ALTER MATERIALIZED VIEW conditions_dist_1m_manual SET ( timescaledb.compress );
SELECT compress_chunk(ch)
FROM show_chunks('conditions_dist_1m_manual') ch limit 1;
SELECT * FROM conditions_dist_1m_manual ORDER BY bucket;

-- Clean up
DROP TABLE conditions_dist CASCADE;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;

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
   timescaledb_experimental.time_bucket_ng('1 month', day) AS bucket,
   MIN(temperature),
   MAX(temperature)
FROM conditions_empty
GROUP BY city, bucket;

SELECT city, to_char(bucket, 'YYYY-MM-DD') AS month, min, max
FROM conditions_summary_empty
ORDER by month, city;

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
ORDER by month, city;

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
   timescaledb_experimental.time_bucket_ng('1 month', day) AS bucket,
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
