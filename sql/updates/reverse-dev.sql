CREATE FUNCTION _timescaledb_internal.create_chunk_table(hypertable REGCLASS, slices JSONB, schema_name NAME, table_name NAME) RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;
CREATE FUNCTION _timescaledb_functions.create_chunk_table(hypertable REGCLASS, slices JSONB, schema_name NAME, table_name NAME) RETURNS BOOL AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;

-- Split chunk
DROP PROCEDURE IF EXISTS @extschema@.split_chunk(chunk REGCLASS, column_name NAME, split_at "any", verbose bool);
