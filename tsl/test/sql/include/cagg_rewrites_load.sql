-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timezone TO PST8PDT;

CREATE FUNCTION text_part_func(TEXT) RETURNS BIGINT
    AS $$ SELECT length($1)::BIGINT $$
    LANGUAGE SQL IMMUTABLE;

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

CREATE TABLE conditions_custom AS SELECT * FROM conditions;
SELECT table_name FROM create_hypertable('conditions_custom', 'city', chunk_time_interval => 6, time_partitioning_func => 'text_part_func', migrate_data => true);

CREATE TABLE devices ( device_id int not null, name text, location text);
INSERT INTO devices values (1, 'thermo_1', 'Moscow'), (2, 'thermo_2', 'Berlin'),(3, 'thermo_3', 'London'),(4, 'thermo_4', 'Stockholm');

CREATE TABLE location (location_id INTEGER, name TEXT);
INSERT INTO location VALUES (1, 'Moscow'), (2, 'Berlin'), (3, 'London'), (4, 'Stockholm');

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
SELECT time_bucket(INTERVAL '3 days', day) AS bucket,
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

-- Create CAgg with more joins and additional WHERE conditions
CREATE MATERIALIZED VIEW cagg_more_conds
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   COUNT(location_id),
   devices.name,
   devices.device_id * 2
FROM conditions LEFT JOIN devices ON conditions.device_id = devices.device_id
JOIN location ON conditions.city = location.name
WHERE location_id > 1 AND
      conditions.temperature > 28
GROUP BY devices.name, bucket, devices.device_id;

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
WITH (timescaledb.continuous, timescaledb.materialized_only=FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
   AVG(temperature),
   devices_view.device_id,
   name
FROM conditions, devices_view
WHERE conditions.device_id = devices_view.device_id
GROUP BY name, bucket, devices_view.device_id;

-- Join on lateral subquery
CREATE MATERIALIZED VIEW cagg_lateral WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE)
AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket, q.name, avg(temperature)  from conditions,
LATERAL (SELECT * FROM devices WHERE devices.device_id = conditions.device_id) q
GROUP BY bucket, q.name;

SELECT h.schema_name AS "MAT_SCHEMA_NAME",
       h.table_name AS "MAT_TABLE_NAME"
FROM _timescaledb_catalog.continuous_agg ca
INNER JOIN _timescaledb_catalog.hypertable h ON(h.id = ca.mat_hypertable_id)
WHERE user_view_name = 'cagg1'
\gset

