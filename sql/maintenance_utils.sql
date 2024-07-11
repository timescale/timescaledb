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
    recompress BOOLEAN = false,
    compress_using NAME = NULL
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_compress_chunk' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION @extschema@.decompress_chunk(
    uncompressed_chunk REGCLASS,
    if_compressed BOOLEAN = true
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_decompress_chunk' LANGUAGE C STRICT VOLATILE;

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

-- A version of makeaclitem that accepts a comma-separated list of
-- privileges rather than just a single privilege. This is copied from
-- PG16, but since we need to support earlier versions, we provide it
-- with the extension.
--
-- This is intended for internal usage and interface might change.
CREATE OR REPLACE FUNCTION _timescaledb_functions.makeaclitem(regrole, regrole, text, bool)
RETURNS AclItem AS '@MODULE_PATHNAME@', 'ts_makeaclitem'
LANGUAGE C STABLE PARALLEL SAFE STRICT;

-- Repair relation ACL by removing roles that do not exist in pg_authid.
CREATE OR REPLACE PROCEDURE _timescaledb_functions.repair_relation_acls()
LANGUAGE SQL AS $$
  WITH
    badrels AS (
	SELECT oid::regclass
	  FROM (SELECT oid, (aclexplode(relacl)).* FROM pg_class) AS rels
	 WHERE rels.grantee != 0
	   AND rels.grantee NOT IN (SELECT oid FROM pg_authid)
    ),
    pickacls AS (
      SELECT oid::regclass,
	     _timescaledb_functions.makeaclitem(
	         b.grantee,
		 b.grantor,
		 string_agg(b.privilege_type, ','),
		 b.is_grantable
	     ) AS acl
	FROM (SELECT oid, (aclexplode(relacl)).* AS a FROM pg_class) AS b
       WHERE b.grantee IN (SELECT oid FROM pg_authid)
       GROUP BY oid, b.grantee, b.grantor, b.is_grantable
    ),
    cleanacls AS (
      SELECT oid, array_agg(acl) AS acl FROM pickacls GROUP BY oid
    )
  UPDATE pg_class c
     SET relacl = (SELECT acl FROM cleanacls n WHERE c.oid = n.oid)
   WHERE oid IN (SELECT oid FROM badrels)
$$ SET search_path TO pg_catalog, pg_temp;

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
    AND dropped IS TRUE
    AND NOT EXISTS (
        SELECT FROM information_schema.tables
        WHERE tables.table_schema = chunk.schema_name
        AND tables.table_name = chunk.table_name
    )
    AND NOT EXISTS (
        SELECT FROM _timescaledb_catalog.hypertable
        JOIN _timescaledb_catalog.continuous_agg ON continuous_agg.raw_hypertable_id = hypertable.id
        WHERE hypertable.id = chunk.hypertable_id
        -- for the old caggs format we need to keep chunk metadata for dropped chunks
        AND continuous_agg.finalized IS FALSE
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

    DELETE FROM _timescaledb_catalog.chunk_index
    WHERE chunk_index.chunk_id = _chunk_id;

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
