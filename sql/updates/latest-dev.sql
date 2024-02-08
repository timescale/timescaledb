-- Remove multi-node CAGG support
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_cagg_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_hyper_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_internal.materialization_invalidation_log_delete(integer);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_invalidation_log_delete(integer);

DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_cagg_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_hyper_log_add_entry(integer,bigint,bigint);
DROP FUNCTION IF EXISTS _timescaledb_functions.materialization_invalidation_log_delete(integer);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_cagg_log(integer,integer,regtype,bigint,bigint,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.invalidation_process_hypertable_log(integer,integer,regtype,integer[],bigint[],bigint[],text[]);
DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_invalidation_log_delete(integer);

-- Remove chunk metadata when marked as dropped
CREATE FUNCTION _timescaledb_functions.remove_dropped_chunk_metadata(_hypertable_id INTEGER)
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
        RETURNING _timescaledb_catalog.dimension_slice.id
    )
    DELETE FROM _timescaledb_catalog.chunk_constraint
    USING _dimension_slice_remove
    WHERE chunk_constraint.dimension_slice_id = _dimension_slice_remove.id;

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

SELECT _timescaledb_functions.remove_dropped_chunk_metadata(id) AS chunks_metadata_removed
FROM _timescaledb_catalog.hypertable;
