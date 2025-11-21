-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timezone TO PST8PDT;

CREATE TABLE conditions(
  day TIMESTAMPTZ NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL,
  device_id int NOT NULL
);
SELECT table_name FROM create_hypertable('conditions', 'day', chunk_time_interval => INTERVAL '1 day');
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-06-14', 'Moscow', 26,1),
  ('2021-06-15', 'Berlin', 22,2),
  ('2021-06-16', 'Stockholm', 24,3),
  ('2021-06-17', 'London', 24,4),
  ('2021-06-18', 'London', 27,4),
  ('2021-06-19', 'Moscow', 28,4),
  ('2021-06-20', 'Moscow', 30,1),
  ('2021-06-21', 'Berlin', 31,1),
  ('2021-06-22', 'Stockholm', 34,1),
  ('2021-06-23', 'Stockholm', 34,2),
  ('2021-06-24', 'Moscow', 34,2),
  ('2021-06-25', 'London', 32,3),
  ('2021-06-26', 'Moscow', 32,3),
  ('2021-06-27', 'Moscow', 31,3);

CREATE TABLE conditions_dup AS SELECT * FROM conditions;
SELECT table_name FROM create_hypertable('conditions_dup', 'day', chunk_time_interval => INTERVAL '1 day', migrate_data => true);

CREATE TABLE devices ( device_id int not null, name text, location text);
INSERT INTO devices values (1, 'thermo_1', 'Moscow'), (2, 'thermo_2', 'Berlin'),(3, 'thermo_3', 'London'),(4, 'thermo_4', 'Stockholm');

CREATE TABLE location (location_id INTEGER, name TEXT);
INSERT INTO location VALUES (1, 'Moscow'), (2, 'Berlin'), (3, 'London'), (4, 'Stockholm');

CREATE TABLE devices_dup AS SELECT * FROM devices;
CREATE VIEW devices_view AS SELECT * FROM devices;

-- 1-table Caggs
CREATE MATERIALIZED VIEW cagg1
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg,
   device_id
FROM conditions
GROUP BY device_id, bucket
ORDER BY bucket;

CREATE MATERIALIZED VIEW cagg2
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '2 days', day) AS bucket,
   AVG(temperature),
   count(device_id)
FROM conditions
GROUP BY bucket;

CREATE MATERIALIZED VIEW cagg3
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '2 days', day) AS bucket,
   AVG(temperature) AS avg,
   count(device_id)
FROM conditions
GROUP BY bucket
HAVING count(device_id) > 1;

-- Caggs with joins
CREATE MATERIALIZED VIEW cagg_join
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions, devices
WHERE conditions.device_id = devices.device_id
GROUP BY name, bucket
ORDER BY bucket;

-- Create CAgg with join and additional WHERE conditions
CREATE MATERIALIZED VIEW cagg_more_conds
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices ON conditions.device_id = devices.device_id
WHERE conditions.city = devices.location AND
      conditions.temperature > 28
GROUP BY name, bucket;

-- Hierarchical CAgg with join
CREATE MATERIALIZED VIEW cagg_on_cagg1
WITH (timescaledb.continuous, timescaledb.materialized_only=FALSE) AS
SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket,
       SUM(avg) AS temperature
FROM cagg1, devices
WHERE devices.device_id = cagg1.device_id
GROUP BY 1;

-- Joining a hypertable and view
CREATE MATERIALIZED VIEW cagg_view
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   devices_view.device_id,
   name
FROM conditions, devices_view
WHERE conditions.device_id = devices_view.device_id
GROUP BY name, bucket, devices_view.device_id;

-- Queries not eligible for rewrite
set timescaledb.cagg_rewrites_debug_info=1;

-- No group by
SELECT 
   AVG(temperature),
   count(device_id)
FROM conditions;

-- no hypertable
SELECT location, count(device_id)
FROM devices
GROUP BY location
ORDER BY 1;

-- no time bucket
SELECT day,
   AVG(temperature),
   count(device_id)
FROM conditions
GROUP BY day
ORDER BY 1;

-- no Caggs
SELECT time_bucket(INTERVAL '3 days', day) AS bucket,
   AVG(temperature),
   count(device_id)
FROM conditions_dup
GROUP BY bucket
ORDER BY 1;

-- Test queries eligibility for rewrites (cagg1)
set timescaledb.enable_cagg_rewrites=0;

SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg,
   device_id
FROM conditions
GROUP BY device_id, bucket
ORDER BY 1;

-- Test query rewrite (cagg1)
set timescaledb.enable_cagg_rewrites=1;

SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature) AS avg,
   device_id
FROM conditions
GROUP BY device_id, bucket
ORDER BY 1;

-- Test queries eligibility for rewrites (cagg2)
set timescaledb.enable_cagg_rewrites=0;

SELECT time_bucket(INTERVAL '2 day', day) AS bucket,
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1
LIMIT 3;

-- Test query rewrite (cagg2)
set timescaledb.enable_cagg_rewrites=1;

SELECT time_bucket(INTERVAL '2 day', day) AS bucket,
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1
LIMIT 3;

-- Make sure Cagg was used
explain SELECT time_bucket(INTERVAL '2 day', day) AS bucket,
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1
LIMIT 3;

DROP MATERIALIZED VIEW cagg_on_cagg1 CASCADE;
DROP MATERIALIZED VIEW cagg1 CASCADE;
DROP MATERIALIZED VIEW cagg2 CASCADE;
DROP MATERIALIZED VIEW cagg3 CASCADE;
DROP MATERIALIZED VIEW cagg_join CASCADE;
DROP MATERIALIZED VIEW cagg_more_conds CASCADE;
DROP MATERIALIZED VIEW cagg_view CASCADE;

DROP TABLE conditions CASCADE;
DROP TABLE conditions_dup CASCADE;
DROP VIEW devices_view CASCADE;
DROP TABLE devices CASCADE;
DROP TABLE devices_dup CASCADE;

DROP TABLE location CASCADE;
