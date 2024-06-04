-- Hyperstore AM
DROP ACCESS METHOD IF EXISTS hsproxy;
DROP FUNCTION IF EXISTS ts_hsproxy_handler;
DROP ACCESS METHOD IF EXISTS hyperstore;
DROP FUNCTION IF EXISTS ts_hyperstore_handler;
DROP FUNCTION IF EXISTS _timescaledb_debug.is_compressed_tid;

DROP FUNCTION IF EXISTS @extschema@.compress_chunk(uncompressed_chunk REGCLASS,	if_not_compressed BOOLEAN, recompress BOOLEAN, compress_using NAME);

CREATE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_compress_chunk' LANGUAGE C STRICT VOLATILE;
