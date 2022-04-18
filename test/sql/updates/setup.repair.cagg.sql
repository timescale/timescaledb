-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- We want to check for inconsistent view generation for code coverage but we do not want to check
-- for differences between clean and upgraded installation because we only issue warnings instead
-- of actually repairing. As a result, if we run update tests instead of repair tests there would
-- be differences and the update tests would fail.
DO $$
DECLARE
 ts_version TEXT;
BEGIN
  SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';
  IF ts_version >= '2.0.0' AND ts_version < '2.7.0' THEN

    CREATE TABLE conditions_v3 (
          timec       TIMESTAMPTZ       NOT NULL,
          temperature DOUBLE PRECISION  NULL,
          humidity    DOUBLE PRECISION  NULL
    );
    PERFORM create_hypertable('conditions_v3', 'timec');

    INSERT INTO conditions_v3
    SELECT generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 55, 75;
    INSERT INTO conditions_v3
    SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 35, 45;
    INSERT INTO conditions_v3
    SELECT generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day'), 73, 55;

    -- Targets that contain Aggref nodes and Var nodes don't generate correct views before version 2.7.0
    CREATE MATERIALIZED VIEW inconsistent_target
    WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
      SELECT time_bucket('1 week', timec) AS bucket,
      temperature,
      CASE
          WHEN temperature < 0 THEN (avg(humidity) + 10)
          ELSE avg(humidity)
      END AS problematic_target
    FROM conditions_v3
    GROUP BY bucket, temperature
    WITH NO DATA;

   END IF;
END
$$;
