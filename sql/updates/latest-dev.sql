DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_table;
DROP FUNCTION IF EXISTS _timescaledb_functions.create_chunk_table;

-- Split chunk
CREATE PROCEDURE @extschema@.split_chunk(
    chunk REGCLASS,
    column_name NAME = NULL,
    split_at "any" = NULL,
    verbose bool = false
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';
