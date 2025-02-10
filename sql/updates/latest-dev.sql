ALTER TABLE _timescaledb_internal.bgw_job_stat_history
    ALTER COLUMN succeeded DROP NOT NULL,
    ALTER COLUMN succeeded DROP DEFAULT;

CREATE FUNCTION _timescaledb_functions.compressed_data_has_nulls(_timescaledb_internal.compressed_data)
    RETURNS BOOL
    LANGUAGE C STRICT IMMUTABLE
    AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

INSERT INTO _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 5, 1, 'COMPRESSION_ALGORITHM_BOOL', 'bool');
