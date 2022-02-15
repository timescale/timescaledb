-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- chunk - the OID of the chunk to be CLUSTERed
-- index - the OID of the index to be CLUSTERed on, or NULL to use the index
--         last used
CREATE OR REPLACE FUNCTION @extschema@.reorder_chunk(
    chunk REGCLASS,
    index REGCLASS=NULL,
    verbose BOOLEAN=FALSE
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_reorder_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.move_chunk(
    chunk REGCLASS,
    destination_tablespace Name,
    index_destination_tablespace Name=NULL,
    reorder_index REGCLASS=NULL,
    verbose BOOLEAN=FALSE
) RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_move_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_compress_chunk' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.decompress_chunk(
    uncompressed_chunk REGCLASS,
    if_compressed BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_decompress_chunk' LANGUAGE C STRICT VOLATILE;

-- Recompress a chunk
--
-- Will give an error if the chunk was not already compressed. In this
-- case, the user should use compress_chunk instead. Note that this
-- function cannot be executed in an explicit transaction since it
-- contains transaction control commands.
--
-- Parameters:
--   chunk: Chunk to recompress.
--   if_not_compressed: Print notice instead of error if chunk is already compressed.
CREATE OR REPLACE PROCEDURE @extschema@.recompress_chunk(chunk REGCLASS,
                                             if_not_compressed BOOLEAN = false)
AS $$
DECLARE
  status INT;
  chunk_name TEXT[];
BEGIN

    -- procedures with SET clause cannot execute transaction
    -- control so we adjust search_path in procedure body
    SET LOCAL search_path TO pg_catalog;

    status := _timescaledb_internal.chunk_status(chunk);

    -- Chunk names are in the internal catalog, but we only care about
    -- the chunk name here.
    chunk_name := parse_ident(chunk::text);
    CASE status
    WHEN 0 THEN
        RAISE EXCEPTION 'call compress_chunk instead of recompress_chunk';
    WHEN 1 THEN
        IF if_not_compressed THEN
            RAISE NOTICE 'nothing to recompress in chunk "%"', chunk_name[array_upper(chunk_name,1)];
            RETURN;
        ELSE
            RAISE EXCEPTION 'nothing to recompress in chunk "%"', chunk_name[array_upper(chunk_name,1)];
        END IF;
    WHEN 3 THEN
        PERFORM @extschema@.decompress_chunk(chunk);
        COMMIT;
        -- SET LOCAL is only active until end of transaction.
        -- While we could use SET at the start of the function we do not
        -- want to bleed out search_path to caller, so we do SET LOCAL
        -- again after COMMIT
        SET LOCAL search_path TO pg_catalog;
    END CASE;
    PERFORM @extschema@.compress_chunk(chunk, if_not_compressed);
END
$$ LANGUAGE plpgsql;
