/*
  creates an index on a chunk replica node.
*/
CREATE OR REPLACE FUNCTION _iobeamdb_internal.create_chunk_replica_node_index(
    schema_name NAME,
    table_name  NAME,
    main_schema_name NAME,
    main_index_name NAME,
    def TEXT
  )
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    index_name NAME;
    prefix     BIGINT;
    sql_code TEXT;
BEGIN
    prefix = nextval('_iobeamdb_catalog.chunk_replica_node_index_name_prefix');
    index_name := format('%s-%s', prefix, main_index_name);

    sql_code := _iobeamdb_internal.get_index_definition_for_table(schema_name, table_name, index_name, def);

    INSERT INTO _iobeamdb_catalog.chunk_replica_node_index (schema_name, table_name, index_name, main_schema_name, main_index_name, definition) VALUES
        (schema_name, table_name, index_name,main_schema_name, main_index_name, sql_code);
END
$BODY$;
