DROP FUNCTION @extschema@.compress_chunk;
CREATE OR REPLACE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;
