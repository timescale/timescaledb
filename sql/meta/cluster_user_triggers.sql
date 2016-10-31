CREATE OR REPLACE FUNCTION _sysinternal.sync_cluster_user()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on field table';
    END IF;

    FOR schema_name IN
    SELECT n.schema_name
    FROM node AS n
    LOOP
        EXECUTE format(
            $$
                INSERT INTO %I.cluster_user SELECT $1.*
            $$,
            schema_name
        )
        USING NEW;
    END LOOP;
    RETURN NEW;
END
$BODY$;

BEGIN;
DROP TRIGGER IF EXISTS trigger_sync_cluster_user
ON cluster_user;
CREATE TRIGGER trigger_sync_cluster_user AFTER INSERT OR UPDATE OR DELETE ON cluster_user
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_cluster_user();
COMMIT;
