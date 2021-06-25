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

CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('7 days', day) AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket;

SELECT to_char(bucket, 'YYYY-MM-DD'), city, min, max
FROM conditions_summary_weekly
ORDER BY bucket;

DROP TABLE conditions CASCADE;
