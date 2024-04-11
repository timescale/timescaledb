-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.


CREATE TABLE conditions(
  time timestamptz NOT NULL,
  city text NOT NULL,
  temperature INT NOT NULL);

SELECT create_hypertable(
  'conditions', 'time',
  chunk_time_interval => INTERVAL '1 day'
);

-- Ensure no CAgg using time_bucket_ng can be created
\set ON_ERROR_STOP 0

-- Regular CAgg
CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('7 days', time, 'UTC') AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket WITH NO DATA;

-- CAgg with origin
CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('7 days', time, '2024-01-16 18:00:00+00') AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket WITH NO DATA;

-- CAgg with origin and timezone
CREATE MATERIALIZED VIEW conditions_summary_weekly
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT city,
       timescaledb_experimental.time_bucket_ng('7 days', time, '2024-01-16 18:00:00+00', 'UTC') AS bucket,
       MIN(temperature),
       MAX(temperature)
FROM conditions
GROUP BY city, bucket WITH NO DATA;

\set ON_ERROR_STOP 1
