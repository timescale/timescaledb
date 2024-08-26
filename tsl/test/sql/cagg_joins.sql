-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set VERBOSITY default

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

-- Cagg with inner join + realtime  aggregate
CREATE MATERIALIZED VIEW cagg_realtime
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions, devices
WHERE conditions.device_id = devices.device_id
GROUP BY name, bucket
ORDER BY bucket;

\d+ cagg_realtime

SELECT * FROM cagg_realtime ORDER BY bucket, name;
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-06-30', 'Moscow', 28, 3);

SELECT * FROM cagg_realtime ORDER BY bucket, name;

-- Cagg with inner join + realtime  aggregate + JOIN clause
CREATE MATERIALIZED VIEW cagg_realtime_join
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices
ON conditions.device_id = devices.device_id
GROUP BY name, bucket
ORDER BY bucket;

\d+ cagg_realtime_join

SELECT * FROM cagg_realtime_join ORDER BY bucket, name;
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-06-30', 'Moscow', 28, 3);

SELECT * FROM cagg_realtime_join ORDER BY bucket, name;

-- Cagg with inner join + realtime  aggregate + USING clause
CREATE MATERIALIZED VIEW cagg_realtime_using
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices
USING (device_id)
GROUP BY name, bucket
ORDER BY bucket;

\d+ cagg_realtime_using

SELECT * FROM cagg_realtime_using ORDER BY bucket;
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-06-30', 'Moscow', 28, 3);

SELECT * FROM cagg_realtime_using ORDER BY bucket, name;

-- Reorder tables in FROM clause
-- Cagg with inner join + realtime aggregate
CREATE MATERIALIZED VIEW cagg_realtime_reorder
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM devices, conditions
WHERE conditions.device_id = devices.device_id
GROUP BY name, bucket
ORDER BY bucket;

\d+ cagg_realtime_reorder

SELECT * FROM cagg_realtime_reorder ORDER BY bucket, name;
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-07-01', 'Moscow', 28, 3);

SELECT * FROM cagg_realtime_reorder ORDER BY bucket, name;

-- Reorder tables in FROM clause
-- Cagg with inner join + realtime  aggregate + JOIN clause
CREATE MATERIALIZED VIEW cagg_realtime_reorder_join
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM devices JOIN conditions
ON conditions.device_id = devices.device_id
GROUP BY name, bucket
ORDER BY bucket;

\d+ cagg_realtime_reorder_join

SELECT * FROM cagg_realtime_reorder_join ORDER BY bucket, name;
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-07-01', 'Moscow', 28, 3);

SELECT *
FROM cagg_realtime_reorder_join;

-- Reorder tables in FROM clause
-- Cagg with inner join + realtime  aggregate + USING clause
CREATE MATERIALIZED VIEW cagg_realtime_reorder_using
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM devices JOIN conditions
USING (device_id)
GROUP BY name, bucket
ORDER BY bucket;

\d+ cagg_realtime_reorder_using

SELECT * FROM cagg_realtime_reorder_using ORDER BY bucket, name;
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2021-07-01', 'Moscow', 28, 3);

SELECT * FROM cagg_realtime_reorder_using ORDER BY bucket, name;

-- Cagg with inner joins - realtime aggregate
CREATE MATERIALIZED VIEW cagg
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name,
   devices.device_id AS thermo_id
FROM conditions, devices
WHERE conditions.device_id = devices.device_id
GROUP BY bucket, name, thermo_id
ORDER BY bucket;

SELECT * FROM cagg ORDER BY bucket, name, thermo_id;

-- Cagg with inner joins - realtime aggregate + JOIN clause
CREATE MATERIALIZED VIEW cagg_join
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices ON conditions.device_id = devices.device_id
GROUP BY bucket,name
ORDER BY bucket;

SELECT * FROM cagg_join ORDER BY bucket, name;

-- Cagg with inner joins - realtime aggregate + USING clause
CREATE MATERIALIZED VIEW cagg_using
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices USING (device_id)
GROUP BY name, bucket
ORDER BY bucket;

SELECT * FROM cagg_using;

-- Reorder tables in FROM clause
-- Cagg with inner joins - realtime aggregate
CREATE MATERIALIZED VIEW cagg_reorder
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM devices, conditions
WHERE conditions.device_id = devices.device_id
GROUP BY bucket, name
ORDER BY bucket;

SELECT * FROM cagg_reorder ORDER BY bucket, name;

-- Cagg with inner joins - realtime aggregate + JOIN clause
CREATE MATERIALIZED VIEW cagg_reorder_join
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM devices JOIN conditions ON conditions.device_id = devices.device_id
GROUP BY name, bucket
ORDER BY bucket;

SELECT * FROM cagg_reorder_join;

-- Cagg with inner joins - realtime aggregate + USING clause
CREATE MATERIALIZED VIEW cagg_reorder_using
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM devices JOIN conditions USING (device_id)
GROUP BY name, bucket
ORDER BY bucket;

SELECT * FROM cagg_reorder_using;

-- Cagg join with another table using FROM ONLY
CREATE MATERIALIZED VIEW cagg_from_only
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM ONLY devices JOIN conditions USING (device_id)
GROUP BY name, bucket
ORDER BY bucket;

SELECT * FROM cagg_from_only;

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

SELECT * FROM cagg_more_conds ORDER BY bucket;

-- Cagg with more conditions and USING clause
CREATE MATERIALIZED VIEW cagg_more_conds_using
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices USING (device_id)
WHERE conditions.city = devices.location AND
      conditions.temperature > 28
GROUP BY name, bucket;

SELECT * FROM cagg_more_conds_using ORDER BY bucket;

-- Hierarchical CAgg with join
CREATE MATERIALIZED VIEW cagg_on_cagg
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket,
       SUM(avg) AS temperature
FROM cagg, devices
WHERE devices.device_id = cagg.thermo_id
GROUP BY 1;

SELECT * FROM cagg_on_cagg;

DROP MATERIALIZED VIEW cagg_on_cagg CASCADE;

-- Nested CAgg over a CAgg with JOIN clause
CREATE MATERIALIZED VIEW cagg_on_cagg_join
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(INTERVAL '1 day', bucket) AS bucket,
       SUM(avg) AS temperature
FROM cagg JOIN devices
ON devices.device_id = cagg.thermo_id
GROUP BY 1;

SELECT * FROM cagg_on_cagg_join;

DROP MATERIALIZED VIEW cagg_on_cagg_join CASCADE;

-- Create CAgg with join and ORDER BY
CREATE MATERIALIZED VIEW cagg_ordered
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices ON conditions.device_id = devices.device_id
WHERE conditions.city = devices.location AND
      conditions.temperature > 28
GROUP BY name, bucket
ORDER BY name;

SELECT * FROM cagg_ordered ORDER BY bucket;

CREATE MATERIALIZED VIEW cagg_ordered_2
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices ON conditions.device_id = devices.device_id
WHERE conditions.city = devices.location AND
      conditions.temperature > 28
GROUP BY name, bucket
ORDER BY name DESC;

SELECT * FROM cagg_ordered_2 ORDER BY bucket;

CREATE MATERIALIZED VIEW cagg_ordered_3
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices ON conditions.device_id = devices.device_id
WHERE conditions.city = devices.location AND
      conditions.temperature > 28
GROUP BY name, bucket
ORDER BY name, bucket;

SELECT * FROM cagg_ordered_3 ORDER BY bucket;

CREATE MATERIALIZED VIEW cagg_cagg
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   devices.device_id device_id,
   name
FROM conditions, devices
WHERE conditions.device_id = devices.device_id
GROUP BY name, bucket, devices.device_id;

--Join between cagg and normal table
CREATE MATERIALIZED VIEW cagg_nested
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', cagg.bucket) AS bucket,
   devices.name
FROM cagg_cagg cagg, devices
WHERE cagg.device_id = devices.device_id
GROUP BY 1,2;

DROP MATERIALIZED VIEW cagg_nested CASCADE;

-- CAgg with multiple join conditions without JOIN clause
CREATE MATERIALIZED VIEW cagg_more_joins_conds
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   MAX(temperature),
   MIN(temperature),
   name
FROM conditions JOIN devices ON conditions.device_id = devices.device_id
AND conditions.city = devices.location AND
      conditions.temperature > 28
GROUP BY name, bucket;

-- With LATERAL multiple tables in new format
CREATE TABLE mat_t1(a integer, b integer, c TEXT);

CREATE MATERIALIZED VIEW mat_m1 WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket, temperature, count(*) from conditions,
LATERAL (SELECT * FROM mat_t1 WHERE mat_t1.a = conditions.temperature) q
GROUP BY bucket, temperature;

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

CREATE TABLE cities(name text, currency text);
INSERT INTO cities VALUES ('Berlin', 'EUR'), ('London', 'PND');

--Error out when FROM clause has sub selects
\set ON_ERROR_STOP 0
CREATE MATERIALIZED VIEW conditions_summary_subselect
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN
(SELECT *
FROM devices
WHERE location in (
   SELECT name
   FROM cities
   WHERE currency = 'EUR')) dev ON conditions.device_id = dev.device_id
GROUP BY name, bucket;

--Error out when WHERE clause has sub selects
CREATE MATERIALIZED VIEW conditions_summary_subselect
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions, devices
WHERE conditions.city IN (
   SELECT location
   FROM devices
   WHERE location in (
      SELECT name
      FROM cities
      WHERE currency = 'EUR'))
AND conditions.device_id = devices.device_id
GROUP BY name, bucket;

CREATE MATERIALIZED VIEW conditions_summary_subselect
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices USING(device_id)
WHERE conditions.city IN (
   SELECT location
   FROM devices
   WHERE location in (
      SELECT name
      FROM cities
      WHERE currency = 'EUR'))
GROUP BY name, bucket;

CREATE MATERIALIZED VIEW conditions_summary_subselect
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions JOIN devices ON conditions.device_id = devices.device_id
WHERE conditions.city IN (
   SELECT location
   FROM devices
   WHERE location in (
      SELECT name
      FROM cities
      WHERE currency = 'EUR'))
GROUP BY name, bucket;

DROP TABLE cities CASCADE;

--Error out when join is between two hypertables
CREATE MATERIALIZED VIEW cagg_ht
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', conditions.day) AS bucket,
   AVG(conditions.temperature)
FROM conditions, conditions_dup
WHERE conditions.device_id = conditions_dup.device_id
GROUP BY bucket;

--Error out when join is between two normal tables
CREATE MATERIALIZED VIEW cagg_nt
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT AVG(devices.device_id),
   devices.name,
   devices.location
FROM devices, devices_dup
WHERE devices.device_id = devices_dup.device_id
GROUP BY devices.name, devices.location;
\set ON_ERROR_STOP 1

-- JOIN with non-equality condition
CREATE MATERIALIZED VIEW cagg_unequal1
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions, devices
WHERE conditions.device_id <> devices.device_id
GROUP BY name, bucket;

-- JOIN with non-equality condition
CREATE MATERIALIZED VIEW cagg_unequal2
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions, devices
WHERE conditions.device_id = devices.device_id AND
      conditions.city like '%cow*'
GROUP BY name, bucket;

-- JOIN with non-equality condition
CREATE MATERIALIZED VIEW cagg_unequal3
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions
JOIN devices ON conditions.device_id = devices.device_id OR conditions.city LIKE '%cow*'
GROUP BY name, bucket;

-- Error out when join type is not INNER or LEFT
\set ON_ERROR_STOP 0
CREATE MATERIALIZED VIEW cagg_outer
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions FULL JOIN devices
ON conditions.device_id = devices.device_id
GROUP BY name, bucket;

CREATE MATERIALIZED VIEW cagg_right
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions RIGHT JOIN devices
ON conditions.device_id = devices.device_id
GROUP BY name, bucket;
\set ON_ERROR_STOP 1

-- LEFT JOIN is allowed
CREATE MATERIALIZED VIEW cagg_left_join
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   name
FROM conditions LEFT JOIN devices
ON conditions.device_id = devices.device_id
GROUP BY name, bucket;

-- Error out for join between cagg and hypertable
\set ON_ERROR_STOP 0
CREATE MATERIALIZED VIEW cagg_nested_ht
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', cagg.bucket) AS bucket,
   cagg.name,
   conditions.temperature
FROM cagg_cagg cagg, conditions
WHERE cagg.device_id = conditions.device_id
GROUP BY 1,2,3;
\set ON_ERROR_STOP 1

-- Multiple JOINS are supported
CREATE MATERIALIZED VIEW conditions_by_day WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 day', conditions.day) AS bucket,
   AVG(conditions.temperature),
   devices.name AS device,
   location.name AS location
FROM conditions
JOIN devices ON conditions.device_id = devices.device_id
JOIN location ON location.name = devices.location
GROUP BY bucket, devices.name, location.name;

SELECT * FROM conditions_by_day ORDER BY bucket, device, location;

ALTER MATERIALIZED VIEW conditions_by_day SET (timescaledb.materialized_only = FALSE);

-- Insert one more row on conditions and check the result (should have one more row)
INSERT INTO conditions (day, city, temperature, device_id) VALUES
  ('2024-07-01', 'Moscow', 28, 3);

SELECT * FROM conditions_by_day ORDER BY bucket, device, location;

-- JOIN with a foreign table
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT current_setting('port') AS "PGPORT", current_database() AS "PGDATABASE" \gset
CREATE EXTENSION postgres_fdw;
CREATE SERVER loopback
   FOREIGN DATA WRAPPER postgres_fdw
   OPTIONS (host 'localhost', dbname :'PGDATABASE', port :'PGPORT');
CREATE USER MAPPING FOR :ROLE_DEFAULT_PERM_USER
   SERVER loopback
   OPTIONS (user :'ROLE_DEFAULT_PERM_USER', password 'nopassword');
ALTER USER MAPPING FOR :ROLE_DEFAULT_PERM_USER
   SERVER loopback
   OPTIONS (ADD password_required 'false');
GRANT USAGE ON FOREIGN SERVER loopback TO :ROLE_DEFAULT_PERM_USER;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER;

CREATE FOREIGN TABLE devices_fdw (
   device_id int not null,
   name text,
   location text
) SERVER loopback OPTIONS (table_name 'devices');

CREATE MATERIALIZED VIEW conditions_fdw WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 day', conditions.day) AS bucket,
   AVG(conditions.temperature),
   devices.name AS device
FROM conditions
JOIN devices_fdw AS devices ON conditions.device_id = devices.device_id
GROUP BY bucket, devices.name;

\set VERBOSITY terse
SET client_min_messages TO WARNING;
DROP TABLE conditions CASCADE;
DROP TABLE devices CASCADE;
DROP TABLE conditions_dup CASCADE;
DROP TABLE devices_dup CASCADE;
RESET client_min_messages;

\set VERBOSITY default

-- SDC #1859
CREATE TABLE conditions(
  time TIMESTAMPTZ NOT NULL,
  value FLOAT8 NOT NULL,
  device_id int NOT NULL
);

SELECT table_name FROM create_hypertable('conditions', 'time', chunk_time_interval => INTERVAL '1 day');

INSERT INTO conditions (time, value, device_id)
SELECT t, 1, 1 FROM generate_series('2024-01-01 00:00:00-00'::timestamptz, '2024-12-31 00:00:00-00'::timestamptz, '1 hour'::interval) AS t;

CREATE TABLE devices (device_id int not null, name text, location text);
INSERT INTO devices values (1, 'thermo_1', 'Moscow'), (2, 'thermo_2', 'Berlin'),(3, 'thermo_3', 'London'),(4, 'thermo_4', 'Stockholm');

CREATE MATERIALIZED VIEW cagg_realtime
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', time) AS bucket,
   MAX(value),
   MIN(value),
   AVG(value),
   devices.name,
   devices.location
FROM conditions
JOIN devices ON conditions.device_id = devices.device_id
GROUP BY name, location, bucket
WITH NO DATA;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
VACUUM ANALYZE;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT a.* FROM cagg_realtime a WHERE a.location = 'Moscow' ORDER BY bucket LIMIT 2;

\set VERBOSITY terse
SET client_min_messages TO WARNING;
DROP TABLE conditions CASCADE;
DROP TABLE devices CASCADE;
RESET client_min_messages;
