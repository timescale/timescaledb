CREATE OR REPLACE FUNCTION _sysinternal.on_create_cluster_user()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    node_row node;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on node table';
    END IF;

    --NOTE:  creating the role should be done outside this purview. Permissions are complex and should be set by the DBA
    -- before creating the cluster user

    FOR node_row IN SELECT *
                    FROM node
                    WHERE database_name <> current_database() LOOP
        PERFORM _sysinternal.create_user_mapping(NEW, node_row);
    END LOOP;
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_cluster_user
ON cluster_user;
CREATE TRIGGER trigger_on_create_cluster_user AFTER INSERT OR UPDATE OR DELETE ON cluster_user
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_cluster_user();
COMMIT;
