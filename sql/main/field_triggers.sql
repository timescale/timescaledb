CREATE OR REPLACE FUNCTION create_field_indexes_on_data_tables(
    namespace_name NAME,
    field_name     NAME,
    index_types    field_index_type []
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _sysinternal.create_data_table_index(dt.table_oid, field_name, index_type)
    FROM data_table dt
    CROSS JOIN unnest(index_types) AS index_type
    WHERE dt.namespace_name = create_field_indexes_on_data_tables.namespace_name;
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
    PERFORM create_field_indexes_on_data_tables(NEW.namespace_name, NEW.name, NEW.index_types);
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
