-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET search_path TO public, _timescaledb_functions;

CREATE TABLE devices (
    id INTEGER,
    name TEXT
);

CREATE TABLE metrics (
    "time" TIMESTAMPTZ NOT NULL,
    device_id INTEGER,
    value FLOAT8
);

SELECT table_name FROM create_hypertable('metrics', 'time');

-- fixed bucket size
CREATE MATERIALIZED VIEW metrics_by_hour WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', time) AS bucket, count(*)
FROM metrics
GROUP BY 1
WITH NO DATA;

-- variable bucket size
CREATE MATERIALIZED VIEW metrics_by_month WITH (timescaledb.continuous) AS
SELECT time_bucket('1 month', bucket) AS bucket, sum(count) AS count
FROM metrics_by_hour
GROUP BY 1
WITH NO DATA;

--
-- ERRORS
--

-- return NULL
SELECT * FROM cagg_validate_query(NULL);

-- nothing to parse
SELECT * FROM cagg_validate_query('');
SELECT * FROM cagg_validate_query('--');
SELECT * FROM cagg_validate_query(';');

-- syntax error
SELECT * FROM cagg_validate_query('blahh');
SELECT * FROM cagg_validate_query($$ SELECT time_bucket(blahh "time") FROM metrics GROUP BY 1 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour' "time") FROM metrics GROUP BY $$);

-- multiple statements are not allowed
SELECT * FROM cagg_validate_query($$ SELECT 1; SELECT 2; $$);

-- only SELECT queries are allowed
SELECT * FROM cagg_validate_query($$ DELETE FROM pg_catalog.pg_class $$);
SELECT * FROM cagg_validate_query($$ UPDATE pg_catalog.pg_class SET relkind = 'r' $$);
SELECT * FROM cagg_validate_query($$ DELETE FROM pg_catalog.pg_class $$);
SELECT * FROM cagg_validate_query($$ VACUUM (ANALYZE) $$);

-- invalid queries
SELECT * FROM cagg_validate_query($$ SELECT 1 $$);
SELECT * FROM cagg_validate_query($$ SELECT 1 FROM pg_catalog.pg_class $$);
SELECT * FROM cagg_validate_query($$ SELECT relkind, count(*) FROM pg_catalog.pg_class GROUP BY 1 $$);

-- time_bucket with offset is not allowed
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time", "offset" => '-1 minute'::interval), count(*) FROM metrics GROUP BY 1 $$);

-- time_bucket with origin is not allowed
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time", origin => '2023-01-01'::timestamptz), count(*) FROM metrics GROUP BY 1 $$);

-- time_bucket with origin is not allowed
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time", origin => '2023-01-01'::timestamptz), count(*) FROM metrics GROUP BY 1 $$);

-- time_bucket_gapfill is not allowed
SELECT * FROM cagg_validate_query($$ SELECT time_bucket_gapfill('1 hour', "time"), count(*) FROM metrics GROUP BY 1 $$);

-- invalid join queries
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', a."time"), count(*) FROM metrics a, metrics b GROUP BY 1 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time"), count(*) FROM metrics, devices a, devices b GROUP BY 1 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time"), device_id, count(*) FROM metrics LEFT JOIN devices ON id = device_id GROUP BY 1, 2 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time"), device_id, count(*) FROM metrics JOIN devices ON id = device_id AND name = 'foo' GROUP BY 1, 2 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time"), device_id, count(*) FROM metrics JOIN devices ON id < device_id GROUP BY 1, 2 $$);

-- invalid caggs on caggs
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('60 days', bucket) AS bucket, sum(count) AS count FROM metrics_by_month GROUP BY 1 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 day 33 minutes', bucket) AS bucket, sum(count) AS count FROM metrics_by_hour GROUP BY 1 $$);

--
-- OK
--

-- valid join queries
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time"), device_id, count(*) FROM metrics JOIN devices ON id = device_id GROUP BY 1, 2 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time"), device_id, count(*) FROM metrics JOIN devices ON id = device_id WHERE devices.name = 'foo' GROUP BY 1, 2 $$);

-- valid queries
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time"), count(*) FROM metrics GROUP BY 1 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time", timezone => 'UTC'), count(*) FROM metrics GROUP BY 1 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 hour', "time", timezone => 'UTC'), count(*) FROM metrics GROUP BY 1 HAVING count(*) > 1 $$);

-- caggs on caggs
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 day', bucket) AS bucket, sum(count) AS count FROM metrics_by_hour GROUP BY 1 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 month', bucket) AS bucket, sum(count) AS count FROM metrics_by_hour GROUP BY 1 $$);
SELECT * FROM cagg_validate_query($$ SELECT time_bucket('1 year', bucket) AS bucket, sum(count) AS count FROM metrics_by_month GROUP BY 1 $$);

--
-- Test bucket Oid recovery
--
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION cagg_get_bucket_function(
    mat_hypertable_id INTEGER
) RETURNS regprocedure AS :MODULE_PATHNAME, 'ts_continuous_agg_get_bucket_function' LANGUAGE C STRICT VOLATILE;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE timestamp_ht (
  time timestamp NOT NULL,
  value float
);

SELECT create_hypertable('timestamp_ht', 'time');

INSERT INTO timestamp_ht
  SELECT time, ceil(random() * 100)::int
    FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,
                         '2000-01-01 23:59:59+0','1m') time;

CREATE MATERIALIZED VIEW temperature_4h
  WITH  (timescaledb.continuous) AS
  SELECT time_bucket('4 hour', time), avg(value)
    FROM timestamp_ht
    GROUP BY 1 ORDER BY 1;

CREATE TABLE timestamptz_ht (
  time timestamptz NOT NULL,
  value float
);

SELECT create_hypertable('timestamptz_ht', 'time');

INSERT INTO timestamptz_ht
  SELECT time, ceil(random() * 100)::int
    FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,
                         '2000-01-01 23:59:59+0','1m') time;

CREATE MATERIALIZED VIEW temperature_tz_4h
  WITH  (timescaledb.continuous) AS
  SELECT time_bucket('4 hour', time), avg(value)
    FROM timestamptz_ht
    GROUP BY 1 ORDER BY 1;

CREATE MATERIALIZED VIEW temperature_tz_4h_ts
  WITH  (timescaledb.continuous) AS
  SELECT time_bucket('4 hour', time, 'Europe/Berlin'), avg(value)
    FROM timestamptz_ht
    GROUP BY 1 ORDER BY 1;

CREATE MATERIALIZED VIEW temperature_tz_4h_ts_ng
  WITH  (timescaledb.continuous) AS
  SELECT timescaledb_experimental.time_bucket_ng('4 hour', time, 'Asia/Shanghai'), avg(value)
    FROM timestamptz_ht
    GROUP BY 1 ORDER BY 1;

CREATE TABLE integer_ht(a integer, b integer, c integer);
SELECT table_name FROM create_hypertable('integer_ht', 'a', chunk_time_interval=> 10);

CREATE OR REPLACE FUNCTION integer_now_integer_ht() returns int LANGUAGE SQL STABLE as $$ SELECT coalesce(max(a), 0) FROM integer_ht $$;
SELECT set_integer_now_func('integer_ht', 'integer_now_integer_ht');

CREATE MATERIALIZED VIEW integer_ht_cagg
  WITH (timescaledb.continuous) AS
  SELECT a, count(b)
     FROM integer_ht
     GROUP BY time_bucket(1, a), a;

--- Get the bucket Oids
SELECT user_view_name,
       cagg_get_bucket_function(mat_hypertable_id)
       FROM _timescaledb_catalog.continuous_agg
       WHERE user_view_name in('temperature_4h', 'temperature_tz_4h', 'temperature_tz_4h_ts', 'temperature_tz_4h_ts_ng', 'integer_ht_cagg')
       ORDER BY user_view_name;

--- Cleanup
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP FUNCTION IF EXISTS cagg_get_bucket_function(INTEGER);
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
