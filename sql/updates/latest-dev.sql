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

DROP VIEW IF EXISTS timescaledb_information.job_stats;

