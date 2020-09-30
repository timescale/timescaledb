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
DROP TABLE metrics;

\set ON_ERROR_STOP 1
