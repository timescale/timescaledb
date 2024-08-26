-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE SCHEMA cagg_join;
CREATE TABLE cagg_join.sensor(
    id SERIAL PRIMARY KEY,
    name TEXT,
    enabled BOOLEAN
);

CREATE TABLE cagg_join.measurement(
    sensor_id INTEGER REFERENCES cagg_join.sensor(id),
    observed TIMESTAMPTZ,
    value FLOAT
);

SELECT create_hypertable('cagg_join.measurement', 'observed');

CREATE MATERIALIZED VIEW cagg_join.measurement_daily
WITH (timescaledb.continuous) AS
-- Column s.name is functionally dependent on s.id (primary key)
SELECT s.id, s.name, time_bucket(interval '1 day', observed) as bucket, avg(value), min(value), max(value)
FROM cagg_join.sensor s
JOIN cagg_join.measurement m on (s.id = m.sensor_id)
GROUP BY s.id, bucket;
