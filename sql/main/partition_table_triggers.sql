CREATE OR REPLACE FUNCTION on_create_partition_table()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    namespace_node_row namespace_node;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on partition_table table';
    END IF;

    SELECT *
    INTO STRICT namespace_node_row
    FROM namespace_node n
    WHERE n.namespace_name = NEW.namespace_name AND
          n.database_name = current_database();

    EXECUTE format(
        $$
                CREATE TABLE IF NOT EXISTS %1$I.%2$I (
                    CONSTRAINT partition CHECK(get_partition_for_key(%4$I::text, %5$L) = %6$L)
                ) INHERITS(%1$I.%3$I)
            $$,
        get_schema_name(NEW.namespace_name), NEW.table_name, namespace_node_row.master_table_name, NEW.partitioning_field,
        NEW.total_partitions, NEW.partition_number
    );
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_partition_table
ON partition_table;
CREATE TRIGGER trigger_on_create_partition_table AFTER INSERT OR UPDATE OR DELETE ON partition_table
FOR EACH ROW EXECUTE PROCEDURE on_create_partition_table();
COMMIT;