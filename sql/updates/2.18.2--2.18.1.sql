UPDATE _timescaledb_internal.bgw_job_stat_history SET succeeded = FALSE WHERE succeeded IS NULL;

ALTER TABLE _timescaledb_internal.bgw_job_stat_history
    ALTER COLUMN succeeded SET NOT NULL,
    ALTER COLUMN succeeded SET DEFAULT FALSE;
