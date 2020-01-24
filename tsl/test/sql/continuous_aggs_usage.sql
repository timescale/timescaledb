-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- TEST SETUP --
\set ON_ERROR_STOP 0
SET client_min_messages TO LOG;

-- START OF USAGE TEST --

--First create your hypertable
CREATE TABLE device_readings (
      observation_time  TIMESTAMPTZ       NOT NULL,
      device_id         TEXT              NOT NULL,
      metric            DOUBLE PRECISION  NOT NULL,
      PRIMARY KEY(observation_time, device_id)
);
SELECT table_name FROM create_hypertable('device_readings', 'observation_time');

--Next, create your continuous aggregate view
CREATE VIEW device_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=true) --This flag is what makes the view continuous
AS
SELECT
  time_bucket('1 hour', observation_time) as bucket, --time_bucket is required
  device_id,
  avg(metric) as metric_avg, --We can use regular aggregates
  max(metric)-min(metric) as metric_spread --We can also use expressions on aggregates and constants
FROM
  device_readings
GROUP BY bucket, device_id; --We have to group by the bucket column, but can also add other group-by columns

--Next, insert some data into the raw hypertable
INSERT INTO device_readings
SELECT ts, 'device_1', (EXTRACT(EPOCH FROM ts)) from generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '30 minutes') ts;
INSERT INTO device_readings
SELECT ts, 'device_2', (EXTRACT(EPOCH FROM ts)) from generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '30 minutes') ts;

--Initially, it will be empty.
SELECT * FROM device_summary;

--Normally, the continuous view will be updated automatically on a schedule but, you can also do it manually.
--We alter max_interval_per_job too since we are not using background workers
ALTER VIEW device_summary SET (timescaledb.max_interval_per_job = '60 day');
SET timescaledb.current_timestamp_mock = '2018-12-31 00:00';
REFRESH MATERIALIZED VIEW device_summary;

--Now you can run selects over your view as normal
SELECT * FROM device_summary WHERE metric_spread = 1800 ORDER BY bucket DESC, device_id LIMIT 10;

--You can view informaton about your continuous aggregates. The meaning of these fields will be explained further down.
\x
SELECT * FROM timescaledb_information.continuous_aggregates;

--You can also view information about your background workers.
--Note: (some fields are empty because there are no background workers used in tests)
SELECT * FROM timescaledb_information.continuous_aggregate_stats;
\x

--
-- Refresh interval
--

-- The refresh interval determines how often the background worker
-- for automatic materialization will run. The default is (2 x bucket_width)
SELECT refresh_interval FROM timescaledb_information.continuous_aggregates;

-- You can change this setting with ALTER VIEW (equivalently, specify in WITH clause of CREATE VIEW)
ALTER VIEW device_summary SET (timescaledb.refresh_interval = '1 hour');
SELECT refresh_interval FROM timescaledb_information.continuous_aggregates;


--
-- Refresh lag
--

-- Materialization have a refresh lag, which means that the materialization will not contain
-- the most up-to-date data.
-- Namely, it will only contain data where: bucket end < (max(time)-refresh_lag)

--By default refresh_lag is 2 x bucket_width
SELECT refresh_lag FROM timescaledb_information.continuous_aggregates;
SELECT max(observation_time) FROM device_readings;
SELECT max(bucket) FROM device_summary;

--You can change the refresh_lag (equivalently, specify in WITH clause of CREATE VIEW)
--Negative values create materialization where the bucket ends after the max of the raw data.
--So to have you data always up-to-date make the refresh_lag (-bucket_width). Note this
--will slow down your inserts because of invalidation.
ALTER VIEW device_summary SET (timescaledb.refresh_lag = '-1 hour');
REFRESH MATERIALIZED VIEW device_summary;
SELECT max(observation_time) FROM device_readings;
SELECT max(bucket) FROM device_summary;

--
-- Invalidations
--

--Changes to the raw table, for values that have already been materialized are propagated asynchronously, after the materialization next runs.
--Before update:
SELECT * FROM device_summary WHERE device_id = 'device_1' and bucket = 'Sun Dec 30 13:00:00 2018 PST';

INSERT INTO device_readings VALUES ('Sun Dec 30 13:01:00 2018 PST', 'device_1', 1.0);

--Change not reflected before materializer runs.
SELECT * FROM device_summary WHERE device_id = 'device_1' and bucket = 'Sun Dec 30 13:00:00 2018 PST';
SET timescaledb.current_timestamp_mock = 'Sun Dec 30 13:01:00 2018 PST';
REFRESH MATERIALIZED VIEW device_summary;
--But is reflected after.
SELECT * FROM device_summary WHERE device_id = 'device_1' and bucket = 'Sun Dec 30 13:00:00 2018 PST';

--
-- Dealing with timezones
--

-- You cannot use any functions that depend on the local timezone setting inside a continuous aggregate.
-- For example you cannot cast to the local time. This is because
-- a timezone setting can alter from user-to-user and thus
-- cannot be materialized.

DROP VIEW device_summary CASCADE;
CREATE VIEW device_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
  time_bucket('1 hour', observation_time) as bucket,
  min(observation_time::timestamp) as min_time, --note the cast to localtime
  device_id,
  avg(metric) as metric_avg,
  max(metric)-min(metric) as metric_spread
FROM
  device_readings
GROUP BY bucket, device_id;
--note the error.

-- You have two options:
-- Option 1: be explicit in your timezone:

DROP VIEW device_summary CASCADE;
CREATE VIEW device_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
  time_bucket('1 hour', observation_time) as bucket,
  min(observation_time AT TIME ZONE 'EST') as min_time, --note the explict timezone
  device_id,
  avg(metric) as metric_avg,
  max(metric)-min(metric) as metric_spread
FROM
  device_readings
GROUP BY bucket, device_id;
DROP VIEW device_summary CASCADE;

-- Option 2: Keep things as TIMESTAMPTZ in the view and convert to local time when
-- querying from the view

DROP VIEW device_summary CASCADE;
CREATE VIEW device_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
SELECT
  time_bucket('1 hour', observation_time) as bucket,
  min(observation_time) as min_time, --this is a TIMESTAMPTZ
  device_id,
  avg(metric) as metric_avg,
  max(metric)-min(metric) as metric_spread
FROM
  device_readings
GROUP BY bucket, device_id;
REFRESH MATERIALIZED VIEW device_summary;
SELECT min(min_time)::timestamp FROM device_summary;

--
-- test just in time aggregate / materialization only view
--

-- hardcoding now to 50 will lead to 30 watermark
CREATE OR REPLACE FUNCTION device_readings_int_now()
  RETURNS INT LANGUAGE SQL STABLE AS
$BODY$
  SELECT 50;
$BODY$;

CREATE TABLE device_readings_int(time int, value float);
SELECT create_hypertable('device_readings_int','time',chunk_time_interval:=10);

SELECT set_integer_now_func('device_readings_int','device_readings_int_now');

CREATE VIEW device_readings_mat_only
  WITH (timescaledb.continuous, timescaledb.materialized_only=true)
AS
  SELECT time_bucket(10,time), avg(value) FROM device_readings_int GROUP BY 1;

CREATE VIEW device_readings_jit
  WITH (timescaledb.continuous, timescaledb.materialized_only=false)
AS
  SELECT time_bucket(10,time), avg(value) FROM device_readings_int GROUP BY 1;

INSERT INTO device_readings_int SELECT i, i*10 FROM generate_series(10,40,10) AS g(i);

-- materialization only should have 0 rows
SELECT * FROM device_readings_mat_only ORDER BY time_bucket;

-- jit aggregate should have 4 rows
SELECT * FROM device_readings_jit ORDER BY time_bucket;

REFRESH MATERIALIZED VIEW device_readings_mat_only;
REFRESH MATERIALIZED VIEW device_readings_jit;

-- materialization only should have 2 rows
SELECT * FROM device_readings_mat_only ORDER BY time_bucket;
-- jit aggregate should have 4 rows
SELECT * FROM device_readings_jit ORDER BY time_bucket;

-- add 2 more rows
INSERT INTO device_readings_int SELECT i, i*10 FROM generate_series(50,60,10) AS g(i);

-- materialization only should have 2 rows
SELECT * FROM device_readings_mat_only ORDER BY time_bucket;
-- jit aggregate should have 6 rows
SELECT * FROM device_readings_jit ORDER BY time_bucket;

-- hardcoding now to 100 will lead to 80 watermark
CREATE OR REPLACE FUNCTION device_readings_int_now()
  RETURNS INT LANGUAGE SQL STABLE AS
$BODY$
  SELECT 100;
$BODY$;

-- refresh should materialize all now
REFRESH MATERIALIZED VIEW device_readings_mat_only;
REFRESH MATERIALIZED VIEW device_readings_jit;

-- materialization only should have 6 rows
SELECT * FROM device_readings_mat_only ORDER BY time_bucket;
-- jit aggregate should have 6 rows
SELECT * FROM device_readings_jit ORDER BY time_bucket;



