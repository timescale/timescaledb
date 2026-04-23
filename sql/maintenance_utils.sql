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

CREATE OR REPLACE FUNCTION _timescaledb_functions.create_compressed_chunk(
    chunk REGCLASS,
    chunk_table REGCLASS,
    uncompressed_heap_size BIGINT,
    uncompressed_toast_size BIGINT,
    uncompressed_index_size BIGINT,
    compressed_heap_size BIGINT,
    compressed_toast_size BIGINT,
    compressed_index_size BIGINT,
    numrows_pre_compression BIGINT,
    numrows_post_compression BIGINT
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_create_compressed_chunk' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.compress_chunk(
    uncompressed_chunk REGCLASS,
    if_not_compressed BOOLEAN = true,
    recompress BOOLEAN = false
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_compress_chunk' LANGUAGE C VOLATILE;

-- Alias for compress_chunk above.
CREATE OR REPLACE PROCEDURE @extschema@.convert_to_columnstore(
    chunk REGCLASS,
    if_not_columnstore BOOLEAN = true,
    recompress BOOLEAN = false
) AS '@MODULE_PATHNAME@', 'ts_compress_chunk' LANGUAGE C;

CREATE OR REPLACE FUNCTION @extschema@.decompress_chunk(
    uncompressed_chunk REGCLASS,
    if_compressed BOOLEAN = true
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_decompress_chunk' LANGUAGE C STRICT VOLATILE;

CREATE OR REPLACE PROCEDURE @extschema@.convert_to_rowstore(
    chunk REGCLASS,
    if_columnstore BOOLEAN = true
) AS '@MODULE_PATHNAME@', 'ts_decompress_chunk' LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.rebuild_columnstore(
    chunk REGCLASS
) AS '@MODULE_PATHNAME@', 'ts_rebuild_columnstore' LANGUAGE C;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.chunk_rewrite_cleanup()
LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_chunk_rewrite_cleanup';

CREATE OR REPLACE PROCEDURE @extschema@.merge_chunks(
   chunk1 REGCLASS, chunk2 REGCLASS, concurrently BOOLEAN = false
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_merge_two_chunks';

CREATE OR REPLACE PROCEDURE @extschema@.merge_chunks(
    chunks REGCLASS[]
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_merge_chunks';

CREATE OR REPLACE PROCEDURE @extschema@.merge_chunks_concurrently(
    chunks REGCLASS[]
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_merge_chunks_concurrently';

CREATE OR REPLACE PROCEDURE @extschema@.split_chunk(
    chunk REGCLASS,
    split_at "any" = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_split_chunk';

CREATE OR REPLACE FUNCTION _timescaledb_functions.recompress_chunk_segmentwise(
    uncompressed_chunk REGCLASS,
    if_compressed BOOLEAN = true
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_recompress_chunk_segmentwise' LANGUAGE C STRICT VOLATILE;

-- find the index on the compressed chunk that can be used to recompress efficiently
-- this index must contain all the segmentby columns and the meta_sequence_number column last
CREATE OR REPLACE FUNCTION _timescaledb_functions.get_compressed_chunk_index_for_recompression(
    uncompressed_chunk REGCLASS
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_get_compressed_chunk_index_for_recompression' LANGUAGE C STRICT VOLATILE;
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

CREATE OR REPLACE PROCEDURE @extschema@.recompress_chunk(chunk REGCLASS, if_not_compressed BOOLEAN = true) LANGUAGE PLPGSQL AS $$
BEGIN
  IF current_setting('timescaledb.enable_deprecation_warnings', true)::bool THEN
    RAISE WARNING 'procedure @extschema@.recompress_chunk(regclass,boolean) is deprecated and the functionality is now included in @extschema@.compress_chunk. this compatibility function will be removed in a future version.';
  END IF;
  PERFORM @extschema@.compress_chunk(chunk, if_not_compressed);
END$$ SET search_path TO pg_catalog,pg_temp;

-- Remove chunk metadata when marked as dropped
CREATE OR REPLACE FUNCTION _timescaledb_functions.remove_dropped_chunk_metadata(_hypertable_id INTEGER)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  _chunk_id INTEGER;
  _removed INTEGER := 0;
BEGIN
  FOR _chunk_id IN
    SELECT id FROM _timescaledb_catalog.chunk
    WHERE hypertable_id = _hypertable_id
    AND NOT EXISTS (
        SELECT FROM information_schema.tables
        WHERE tables.table_schema = chunk.schema_name
        AND tables.table_name = chunk.table_name
    )
  LOOP
    _removed := _removed + 1;
    RAISE INFO 'Removing metadata of chunk % from hypertable %', _chunk_id, _hypertable_id;

    WITH _dimension_slice_remove AS (
        DELETE FROM _timescaledb_catalog.dimension_slice
        USING _timescaledb_catalog.chunk_constraint
        WHERE dimension_slice.id = chunk_constraint.dimension_slice_id
        AND chunk_constraint.chunk_id = _chunk_id
        AND NOT EXISTS (
            SELECT FROM _timescaledb_catalog.chunk_constraint cc
            WHERE cc.chunk_id <> _chunk_id
            AND cc.dimension_slice_id = dimension_slice.id
        )
        RETURNING _timescaledb_catalog.dimension_slice.id
    )
    DELETE FROM _timescaledb_catalog.chunk_constraint
    USING _dimension_slice_remove
    WHERE chunk_constraint.dimension_slice_id = _dimension_slice_remove.id;

    DELETE FROM _timescaledb_catalog.chunk_constraint
    WHERE chunk_constraint.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_internal.bgw_policy_chunk_stats
    WHERE bgw_policy_chunk_stats.chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.compression_chunk_size
    WHERE compression_chunk_size.chunk_id = _chunk_id
    OR compression_chunk_size.compressed_chunk_id = _chunk_id;

    DELETE FROM _timescaledb_catalog.chunk
    WHERE chunk.id = _chunk_id
    OR chunk.compressed_chunk_id = _chunk_id;
  END LOOP;

  RETURN _removed;
END;
$$ SET search_path TO pg_catalog, pg_temp;
