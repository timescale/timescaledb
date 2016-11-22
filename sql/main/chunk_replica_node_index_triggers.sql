CREATE OR REPLACE FUNCTION _sysinternal.on_create_chunk_replica_node_index()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    IF NEW.index_type = 'TIME-VALUE' THEN
        EXECUTE format(
            $$
                CREATE INDEX %1$I ON %2$I.%3$I
                USING BTREE(time DESC NULLS LAST, %4$I)
                WHERE %4$I IS NOT NULL
            $$,
            NEW.index_name, NEW.schema_name, NEW.table_name, NEW.field_name);
    ELSIF NEW.index_type = 'VALUE-TIME' THEN
        EXECUTE format(
            $$
                CREATE INDEX %1$I ON %2$I.%3$I
                USING BTREE(%4$I, time DESC NULLS LAST)
                WHERE %4$I IS NOT NULL
            $$,
            NEW.index_name, NEW.schema_name, NEW.table_name, NEW.field_name);
    ELSE
        RAISE EXCEPTION 'Unknown index type %', index_type
        USING ERRCODE = 'IO103';
    END IF;
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_chunk_replica_node_index
ON chunk_replica_node_index;
CREATE TRIGGER trigger_on_create_chunk_replica_node_index AFTER INSERT OR UPDATE OR DELETE ON chunk_replica_node_index
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_chunk_replica_node_index();
COMMIT;
