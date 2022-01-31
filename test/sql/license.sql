-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

\set ECHO queries
\set VERBOSITY default

SHOW timescaledb.license;
SELECT _timescaledb_internal.tsl_loaded();

-- User shouldn't be able to change the license in the session
\set ON_ERROR_STOP 0
SET timescaledb.license='apache';
SET timescaledb.license='timescale';
SET timescaledb.license='something_else';
\set ON_ERROR_STOP 1

-- make sure apache license blocks tsl features
\set ON_ERROR_STOP 0

SELECT locf(1);
SELECT interpolate(1);
SELECT time_bucket_gapfill(1,1,1,1);

CREATE OR REPLACE FUNCTION custom_func(jobid int, args jsonb) RETURNS VOID AS $$
DECLARE
BEGIN
END;
$$ LANGUAGE plpgsql;
SELECT add_job('custom_func','1h', config:='{"type":"function"}'::jsonb);
DROP FUNCTION custom_func;

CREATE TABLE metrics(time timestamptz NOT NULL, value float);
SELECT create_hypertable('metrics', 'time');
ALTER TABLE metrics SET (timescaledb.compress);

INSERT INTO metrics
VALUES ('2022-01-01 00:00:00', 1), ('2022-01-01 01:00:00', 2), ('2022-01-01 02:00:00', 3);

CREATE MATERIALIZED VIEW metrics_hourly
WITH (timescaledb.continuous) AS
SELECT time_bucket(INTERVAL '1 hour', time) AS bucket,
   AVG(value),
   MAX(value),
   MIN(value)
FROM metrics
GROUP BY bucket
WITH NO DATA;

CREATE MATERIALIZED VIEW metrics_hourly
AS
SELECT time_bucket(INTERVAL '1 hour', time) AS bucket,
   AVG(value),
   MAX(value),
   MIN(value)
FROM metrics
GROUP BY bucket;

CALL refresh_continuous_aggregate('metrics_hourly', NULL, NULL);

SELECT _timescaledb_internal.invalidation_hyper_log_add_entry(0, 0, 0);
SELECT _timescaledb_internal.invalidation_cagg_log_add_entry(0, 0, 0);

DROP MATERIALIZED VIEW metrics_hourly;
DROP TABLE metrics;
\set ON_ERROR_STOP 1
