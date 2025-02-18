UPDATE _timescaledb_internal.bgw_job_stat_history SET succeeded = FALSE WHERE succeeded IS NULL;

ALTER TABLE _timescaledb_internal.bgw_job_stat_history
    ALTER COLUMN succeeded SET NOT NULL,
    ALTER COLUMN succeeded SET DEFAULT FALSE;

DROP FUNCTION IF EXISTS _timescaledb_functions.compressed_data_has_nulls(_timescaledb_internal.compressed_data);

DELETE FROM _timescaledb_catalog.compression_algorithm WHERE id = 5 AND version = 1 AND name = 'COMPRESSION_ALGORITHM_BOOL';
