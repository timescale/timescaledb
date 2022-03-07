CREATE FUNCTION _timescaledb_internal.relation_size(relation REGCLASS)
RETURNS TABLE (total_size BIGINT, heap_size BIGINT, index_size BIGINT, toast_size BIGINT)
AS '@MODULE_PATHNAME@', 'ts_relation_size' LANGUAGE C VOLATILE;

DROP VIEW IF EXISTS _timescaledb_internal.hypertable_chunk_local_size;
