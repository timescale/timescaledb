CREATE OR REPLACE FUNCTION _sysinternal.create_chunk_replica_node_index(
    schema_name NAME,
    table_name NAME,
    field_name NAME,
    index_type field_index_type
)
    RETURNS VOID LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    index_name NAME;
    prefix     BIGINT;
BEGIN
    prefix = nextval('chunk_replica_node_index_name_prefix');
    IF index_type = 'TIME-VALUE' THEN
        index_name := format('%s-time-%s', prefix, field_name);
    ELSIF index_type = 'VALUE-TIME' THEN
        index_name := format('%s-%s-time', prefix, field_name);
    ELSE
        RAISE EXCEPTION 'Unknown index type %', index_type
        USING ERRCODE = 'IO103';
    END IF;

    INSERT INTO chunk_replica_node_index (schema_name, table_name, field_name, index_name, index_type) VALUES
        (schema_name, table_name, field_name, index_name, index_type)
    ON CONFLICT DO NOTHING;
END
$BODY$;
