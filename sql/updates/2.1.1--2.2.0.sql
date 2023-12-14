DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;

-- only do stub here to not introduce dependency in shared object in update chain
CREATE FUNCTION @extschema@.create_distributed_restore_point(
    name                   TEXT
) RETURNS TABLE(node_name NAME, node_type TEXT, restore_point pg_lsn)
AS $$SELECT NULL::name,NULL::text,NULL::pg_lsn;$$ LANGUAGE SQL VOLATILE STRICT;

