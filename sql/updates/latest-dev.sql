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
