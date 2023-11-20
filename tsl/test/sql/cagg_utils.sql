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