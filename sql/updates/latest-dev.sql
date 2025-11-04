DO $$
BEGIN
    UPDATE _timescaledb_config.bgw_job
      SET config = config || '{"max_successes_per_job": 1000, "max_failures_per_job": 1000}',
          schedule_interval = '6 hours'
    WHERE id = 3; -- system job retention

    RAISE WARNING 'job history configuration modified'
    USING DETAIL = 'The job history will only keep the last 1000 successes and failures and run once each day.';
END
$$;

CREATE FUNCTION _timescaledb_functions.bloom1_contains_any(_timescaledb_internal.bloom1, anyarray)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

