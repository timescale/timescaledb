CREATE FUNCTION _timescaledb_functions.compressed_data_info(_timescaledb_internal.compressed_data)
    RETURNS TABLE (algorithm name, has_nulls bool)
    AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
    LANGUAGE C STRICT IMMUTABLE SET search_path = pg_catalog, pg_temp;

CREATE INDEX compression_chunk_size_idx ON _timescaledb_catalog.compression_chunk_size (compressed_chunk_id);

CREATE FUNCTION _timescaledb_functions.drop_osm_chunk(hypertable REGCLASS)
	RETURNS BOOL
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
	LANGUAGE C VOLATILE;
