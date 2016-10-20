CREATE OR REPLACE FUNCTION create_field_on_data_tables(
    schema_name        NAME,
    cluster_table_name NAME,
    field              NAME,
    data_type_oid      REGTYPE,
    is_partitioning    BOOLEAN,
    index_types        field_index_type []
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    --TODO:!!!!
END
$BODY$;

CREATE OR REPLACE FUNCTION create_field_on_cluster_table(
    schema_name        NAME,
    cluster_table_name NAME,
    field              NAME,
    data_type_oid      REGTYPE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    EXECUTE format(
        $$
            ALTER TABLE %1$I.%2$I ADD COLUMN %3$I %4$s DEFAULT NULL
        $$,
        schema_name, cluster_table_name, field, data_type_oid);

END
$BODY$;


CREATE OR REPLACE FUNCTION on_create_field()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    namespace_row namespace;
    node_row      node;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on namespace table';
    END IF;

    SELECT *
    INTO STRICT namespace_row
    FROM namespace AS ns
    WHERE ns.name = NEW.namespace_name;


    PERFORM create_field_on_cluster_table(namespace_row.schema_name, namespace_row.cluster_table_name, NEW.name,
                                          NEW.data_type);
    PERFORM create_field_on_data_tables(namespace_row.schema_name, namespace_row.cluster_table_name, NEW.name,
                                        NEW.data_type, NEW.is_partitioning, NEW.index_types);
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_field
ON field;
CREATE TRIGGER trigger_on_create_field AFTER INSERT OR UPDATE OR DELETE ON field
FOR EACH ROW EXECUTE PROCEDURE on_create_field();
COMMIT;