-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.
CREATE TABLE jit_test(time timestamp NOT NULL, device int, temp float);
SELECT create_hypertable('jit_test', 'time');
ALTER TABLE jit_test DROP COLUMN device;

CREATE TABLE jit_test_interval(id int NOT NULL, temp float);
SELECT create_hypertable('jit_test_interval', 'id', chunk_time_interval => 10);

CREATE TABLE jit_test_contagg (
  observation_time  TIMESTAMPTZ       NOT NULL,
  device_id         TEXT              NOT NULL,
  metric            DOUBLE PRECISION  NOT NULL,
  PRIMARY KEY(observation_time, device_id)
);
SELECT table_name FROM create_hypertable('jit_test_contagg', 'observation_time');

CREATE VIEW jit_device_summary
WITH (timescaledb.continuous)
AS
SELECT
  time_bucket('1 hour', observation_time) as bucket,
  device_id,
  avg(metric) as metric_avg,
  max(metric)-min(metric) as metric_spread
FROM
  jit_test_contagg
GROUP BY bucket, device_id;

INSERT INTO jit_test_contagg
SELECT ts, 'device_1', (EXTRACT(EPOCH FROM ts)) from generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '30 minutes') ts;
INSERT INTO jit_test_contagg
SELECT ts, 'device_2', (EXTRACT(EPOCH FROM ts)) from generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '30 minutes') ts;

ALTER VIEW jit_device_summary SET (timescaledb.max_interval_per_job = '60 day');
SET timescaledb.current_timestamp_mock = '2018-12-31 00:00';
REFRESH MATERIALIZED VIEW jit_device_summary;
