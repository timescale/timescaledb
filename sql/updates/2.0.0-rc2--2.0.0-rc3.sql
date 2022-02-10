DROP FUNCTION IF EXISTS @extschema@.add_data_node;

CREATE FUNCTION @extschema@.add_data_node(
    node_name              NAME,
    host                   TEXT,
    database               NAME = NULL,
    port                   INTEGER = NULL,
    if_not_exists          BOOLEAN = FALSE,
    bootstrap              BOOLEAN = TRUE,
    password               TEXT = NULL
) RETURNS TABLE(node_name NAME, host TEXT, port INTEGER, database NAME,
                node_created BOOL, database_created BOOL, extension_created BOOL)
AS '@MODULE_PATHNAME@', 'ts_data_node_add' LANGUAGE C VOLATILE;
