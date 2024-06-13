CREATE FUNCTION _timescaledb_functions.compressed_data_info(_timescaledb_internal.compressed_data)
    RETURNS TABLE (algorithm name, has_nulls bool)
    AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
    LANGUAGE C STRICT IMMUTABLE SET search_path = pg_catalog, pg_temp;

DROP FUNCTION IF EXISTS _timescaledb_functions.chunk_constraint_add_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint
);
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_constraint_add_table_constraint(
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint
);
