ALTER TABLE _timescaledb_internal.bgw_job_stat_history
    ALTER COLUMN succeeded DROP NOT NULL,
    ALTER COLUMN succeeded DROP DEFAULT;
