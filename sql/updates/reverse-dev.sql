-- Drop the type used by the bloom sparse indexes on compressed hypertables.
DROP TYPE _timescaledb_internal.bloom1 CASCADE;


CREATE FUNCTION _timescaledb_internal.create_chunk_table(hypertable REGCLASS, slices JSONB, schema_name NAME, table_name NAME) RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;
CREATE FUNCTION _timescaledb_functions.create_chunk_table(hypertable REGCLASS, slices JSONB, schema_name NAME, table_name NAME) RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;
