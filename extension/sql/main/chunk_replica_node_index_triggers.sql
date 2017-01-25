/*
  Trigger to create/drop indexes on chunk tables when the corresponding
  chunk_replica_node_index row is created/deleted.
*/
CREATE OR REPLACE FUNCTION _iobeamdb_internal.on_create_chunk_replica_node_index()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'Only inserts/deletes supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    IF TG_OP = 'INSERT' THEN
        EXECUTE NEW.definition;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', OLD.schema_name, OLD.index_name);
        RETURN OLD;
    END IF;
END
$BODY$
SET SEARCH_PATH = 'public';
