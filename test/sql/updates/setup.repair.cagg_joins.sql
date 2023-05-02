-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT current_setting('server_version_num')::int >=  130000 AS has_cagg_join_using \gset

CREATE TABLE ht_cagg_joins(
  day TIMESTAMPTZ NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL,
device_id int NOT NULL);
SELECT create_hypertable(
  'ht_cagg_joins', 'day',
  chunk_time_interval => INTERVAL '1 day'
);
INSERT INTO ht_cagg_joins (day, city, temperature, device_id) VALUES
  ('2021-06-14 00:00:00-00'::timestamptz, 'Moscow', 26,1),
  ('2021-06-15 00:00:00-00'::timestamptz, 'Moscow', 22,2),
  ('2021-06-16 00:00:00-00'::timestamptz, 'Berlin', 24,3),
  ('2021-06-17 00:00:00-00'::timestamptz, 'London', 24,4),
  ('2021-06-18 00:00:00-00'::timestamptz, 'Stockholm', 27,4),
  ('2021-06-19 00:00:00-00'::timestamptz, 'Moscow', 28,4),
  ('2021-06-20 00:00:00-00'::timestamptz, 'Stockholm', 30,1),
  ('2021-06-21 00:00:00-00'::timestamptz, 'London', 31,1),
  ('2021-06-22 00:00:00-00'::timestamptz, 'Stockholm', 34,1),
  ('2021-06-23 00:00:00-00'::timestamptz, 'Moscow', 34,2),
  ('2021-06-24 00:00:00-00'::timestamptz, 'London', 34,2),
  ('2021-06-25 00:00:00-00'::timestamptz, 'Stockholm', 32,3),
  ('2021-06-26 00:00:00-00'::timestamptz, 'Berlin', 32,3),
  ('2021-06-27 00:00:00-00'::timestamptz, 'Stockholm', 31,3);

CREATE TABLE nt_cagg_joins ( device_id int not null, name text, location text);
INSERT INTO nt_cagg_joins values (1, 'thermo_1', 'Moscow'), (2, 'thermo_2', 'Berlin'),(3, 'thermo_3', 'London'),(4, 'thermo_4', 'Stockholm');

--Create a cagg with join between a hypertable and a normal table
-- with equality condition on inner join type and realtime aggregation enabled
CREATE MATERIALIZED VIEW cagg_joins_upgrade_test_with_realtime
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
  AVG(temperature),
  name
FROM ht_cagg_joins JOIN nt_cagg_joins
ON ht_cagg_joins.device_id = nt_cagg_joins.device_id
GROUP BY 1,3;

--Create a cagg with join between a hypertable and a normal table
-- with equality condition on inner join type and realtime aggregation disabled
CREATE MATERIALIZED VIEW cagg_joins_upgrade_test
WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
  AVG(temperature),
  name
FROM ht_cagg_joins JOIN nt_cagg_joins
ON ht_cagg_joins.device_id = nt_cagg_joins.device_id
GROUP BY 1,3;

--Create a Cagg with JOIN and additional WHERE conditions
CREATE MATERIALIZED VIEW cagg_joins_where
WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
  AVG(temperature),
  name
FROM ht_cagg_joins JOIN nt_cagg_joins
ON ht_cagg_joins.device_id = nt_cagg_joins.device_id
WHERE ht_cagg_joins.city = nt_cagg_joins.location
GROUP BY 1,3;

-- Only test joins with using clause for postgresql versions above 12
\if :has_cagg_join_using
    --Create a cagg with join with using clause and realtime aggregation enabled
    CREATE MATERIALIZED VIEW cagg_joins_upgrade_test_with_realtime_using
    WITH (timescaledb.continuous, timescaledb.materialized_only = FALSE) AS
    SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
    AVG(temperature),
    name
    FROM ht_cagg_joins JOIN nt_cagg_joins
    USING (device_id)
    GROUP BY 1,3;

    --Create a cagg with join with using and realtime aggregation disabled
    CREATE MATERIALIZED VIEW cagg_joins_upgrade_test_using
    WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE) AS
    SELECT time_bucket(INTERVAL '1 day', day) AS bucket,
    AVG(temperature),
    name
    FROM ht_cagg_joins JOIN nt_cagg_joins
    USING (device_id)
    GROUP BY 1,3;

\endif
