UPDATE _timescaledb_config.bgw_job
  SET config = config || '{"max_successes_per_job": 1000, "max_failures_per_job": 1000}',
      schedule_interval = '1 day'
WHERE proc_schema = '_timescaledb_functions'
  AND proc_name = 'policy_job_stat_history_retention';

DROP VIEW IF EXISTS timescaledb_information.job_stats;

