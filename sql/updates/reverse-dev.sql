DROP FUNCTION _timescaledb_functions.ts_bloom1_matches(bytea, anyelement);


DROP FUNCTION IF EXISTS _timescaledb_functions.compressed_data_has_nulls(_timescaledb_internal.compressed_data);

DELETE FROM _timescaledb_catalog.compression_algorithm WHERE id = 5 AND version = 1 AND name = 'COMPRESSION_ALGORITHM_BOOL';
