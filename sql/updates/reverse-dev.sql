DO $$
BEGIN
    UPDATE _timescaledb_config.bgw_job
      SET config = config - 'max_successes_per_job' - 'max_failures_per_job',
          schedule_interval = '1 month'
    WHERE id = 3; -- system job retention

    RAISE WARNING 'job history configuration modified'
    USING DETAIL = 'The job history will keep full history for each job and run once each month.';
END
$$;

DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

