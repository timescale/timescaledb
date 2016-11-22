CREATE OR REPLACE FUNCTION _sysinternal.on_create_chunk_replica_node_meta()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    INSERT INTO distinct_replica_node (hypertable_name, replica_id, database_name, schema_name, table_name)
        SELECT
            pr.hypertable_name,
            pr.replica_id,
            NEW.database_name,
            h.associated_schema_name,
            format('%s_%s_%s_distinct_data', h.associated_table_prefix, pr.replica_id, n.id)
        FROM partition_replica pr
        INNER JOIN hypertable h ON (h.name = pr.hypertable_name)
        INNER JOIN node n ON (n.database_name = NEW.database_name)
        WHERE pr.id = NEW.partition_replica_id
    ON CONFLICT DO NOTHING;

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_chunk_replica_node
ON chunk_replica_node;
CREATE TRIGGER trigger_on_create_chunk_replica_node AFTER INSERT OR UPDATE OR DELETE ON chunk_replica_node
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_chunk_replica_node_meta();
COMMIT;
