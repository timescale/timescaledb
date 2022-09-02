-- drop and recreate decompress_chunk
DROP FUNCTION IF EXISTS @extschema@.decompress_chunk;
CREATE FUNCTION @extschema@.decompress_chunk(
    uncompressed_chunk REGCLASS,
    if_compressed BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_decompress_chunk' LANGUAGE C STRICT VOLATILE;