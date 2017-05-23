-- Convert a general index definition to a create index sql command for a
-- particular table and index name.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_index_definition_for_table(
    schema_name NAME,
    table_name  NAME,
    index_name NAME,
    general_defintion TEXT
  )
    RETURNS TEXT LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    sql_code TEXT;
BEGIN
    sql_code := replace(general_defintion, '/*TABLE_NAME*/', format('%I.%I', schema_name, table_name));
    sql_code = replace(sql_code, '/*INDEX_NAME*/', format('%I', index_name));

    RETURN sql_code;
END
$BODY$;

-- Creates a chunk_index_row.
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_index_row(
    schema_name NAME,
    table_name  NAME,
    main_schema_name NAME,
    main_index_name NAME,
    def TEXT
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    index_name  NAME;
    id          BIGINT;
    sql_code    TEXT;
BEGIN
    id = nextval(pg_get_serial_sequence('_timescaledb_catalog.chunk_index','id'));
    index_name := format('%s-%s', id, main_index_name);

    sql_code := _timescaledb_internal.get_index_definition_for_table(schema_name, table_name, index_name, def);

    INSERT INTO _timescaledb_catalog.chunk_index (id, schema_name, table_name, index_name, main_schema_name, 
        main_index_name, definition)
    VALUES (id, schema_name, table_name, index_name,main_schema_name, main_index_name, sql_code);
END
$BODY$;
