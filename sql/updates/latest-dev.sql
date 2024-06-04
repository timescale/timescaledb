CREATE FUNCTION _timescaledb_functions.compressed_data_info(_timescaledb_internal.compressed_data)
    RETURNS TABLE (algorithm name, has_nulls bool)
    AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
    LANGUAGE C STRICT IMMUTABLE SET search_path = pg_catalog, pg_temp;

CREATE INDEX compression_chunk_size_idx ON _timescaledb_catalog.compression_chunk_size (compressed_chunk_id);

CREATE FUNCTION _timescaledb_functions.drop_osm_chunk(hypertable REGCLASS)
	RETURNS BOOL
	AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
	LANGUAGE C VOLATILE;

-- Hyperstore updates
CREATE FUNCTION _timescaledb_debug.is_compressed_tid(tid) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C STRICT;

DROP FUNCTION IF EXISTS @extschema@.compress_chunk(uncompressed_chunk REGCLASS,	if_not_compressed BOOLEAN, recompress BOOLEAN);

CREATE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false,
    compress_using NAME = NULL
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;
