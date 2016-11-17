CREATE OR REPLACE FUNCTION _sysinternal.on_create_meta()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    cluster_user_row cluster_user;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    PERFORM _sysinternal.create_server(NEW.server_name, NEW.hostname, NEW.database_name);

    FOR cluster_user_row IN SELECT *
                            FROM cluster_user LOOP
        PERFORM _sysinternal.create_user_mapping(cluster_user_row, NEW.server_name);
    END LOOP;
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_meta 
ON meta;
CREATE TRIGGER trigger_on_create_meta AFTER INSERT OR UPDATE OR DELETE ON meta
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_meta();
COMMIT;
