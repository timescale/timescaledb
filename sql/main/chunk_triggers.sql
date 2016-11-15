CREATE OR REPLACE FUNCTION _sysinternal.on_create_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP <> 'INSERT' AND TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'Only inserts and updates supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    IF TG_OP = 'UPDATE' THEN
       PERFORM _sysinternal.set_time_constraint(crn.schema_name, crn.table_name, NEW.start_time, NEW.end_time) 
        FROM chunk_replica_node crn
        WHERE crn.chunk_id = NEW.id;
    END IF;

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_chunk
ON chunk;
CREATE TRIGGER  trigger_on_create_chunk AFTER INSERT OR UPDATE OR DELETE ON chunk
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_chunk();
COMMIT;
