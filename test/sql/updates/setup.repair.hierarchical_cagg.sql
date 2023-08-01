-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE test (
  timestamp TIMESTAMPTZ NOT NULL,
  device_id TEXT NOT NULL,
  value INT NOT NULL,

  CONSTRAINT uk_test_timestamp_device_id UNIQUE (timestamp, device_id)
);

SELECT create_hypertable('test', 'timestamp');

INSERT INTO test (timestamp, device_id, value) VALUES
  ('2023-05-01 00:00:00+00', 'sensor0', 1),
  ('2023-05-15 00:00:00+00', 'sensor0', 2),
  ('2023-05-31 00:00:00+00', 'sensor0', 10);


CREATE MATERIALIZED VIEW  agg_test_hourly WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 hour'::interval, timestamp) AS hour_timestamp,
        device_id,
        SUM(value)
    FROM test
    GROUP BY hour_timestamp, device_id
WITH DATA;

CREATE MATERIALIZED VIEW agg_test_daily WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 day'::interval, hour_timestamp) AS day_timestamp,
        device_id,
        SUM(sum)
    FROM agg_test_hourly
    GROUP BY day_timestamp, device_id
WITH DATA;

CREATE MATERIALIZED VIEW agg_test_monthly WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 month'::interval, day_timestamp) AS month_timestamp,
        device_id,
        SUM(sum)
    FROM agg_test_daily
    GROUP BY month_timestamp, device_id
WITH DATA;
