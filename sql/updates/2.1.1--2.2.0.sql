DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
DROP VIEW IF EXISTS _timescaledb_internal.compressed_chunk_stats;

CREATE FUNCTION @extschema@.create_distributed_restore_point(
    name                   TEXT
) RETURNS TABLE(node_name NAME, node_type TEXT, restore_point pg_lsn)
AS '@MODULE_PATHNAME@', 'ts_create_distributed_restore_point' LANGUAGE C VOLATILE STRICT;

