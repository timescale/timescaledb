CREATE OR REPLACE FUNCTION add_node(
    database_name NAME,
    server_name   NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    schema_name := format('remote_%s', database_name);
    INSERT INTO node (database_name, schema_name, server_name) VALUES (database_name, schema_name, server_name);
END
$BODY$;


CREATE OR REPLACE FUNCTION add_namespace(
    namespace_name NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
INSERT INTO namespace (name, schema_name, cluster_table_name, cluster_distinct_table_name)
VALUES (namespace_name, get_schema_name(namespace_name), get_cluster_table_name(namespace_name),
        get_cluster_distinct_table_name(namespace_name))
ON CONFLICT DO NOTHING;

INSERT INTO namespace_node (namespace_name, database_name, master_table_name, remote_table_name,
                            distinct_local_table_name, distinct_remote_table_name)
    SELECT
        namespace_name,
        n.database_name,
        get_master_table_name(namespace_name),
        get_remote_table_name(namespace_name, n),
        get_local_distinct_table_name(namespace_name),
        get_remote_distinct_table_name(namespace_name, n)
    FROM node AS n
ON CONFLICT DO NOTHING;
$BODY$;


CREATE OR REPLACE FUNCTION add_field(
    namespace_name  NAME,
    field_name      NAME,
    data_type       REGTYPE,
    is_partitioning BOOLEAN,
    is_distinct     BOOLEAN,
    idx_types       field_index_type []
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
INSERT INTO field (namespace_name, name, data_type, is_partitioning, is_distinct, index_types)
VALUES (namespace_name, field_name, data_type, is_partitioning, is_distinct, idx_types)
ON CONFLICT DO NOTHING;
$BODY$;

