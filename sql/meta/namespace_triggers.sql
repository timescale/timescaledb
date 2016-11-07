CREATE OR REPLACE FUNCTION _sysinternal.sync_namespace()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on namespace table'
        USING ERRCODE = 'IO101';
    END IF;

    FOR schema_name IN
    SELECT n.schema_name
    FROM node AS n
    LOOP
        EXECUTE format(
            $$
                INSERT INTO %I.namespace SELECT $1.*
            $$,
            schema_name
        )
        USING NEW;
    END LOOP;
    RETURN NEW;
END
$BODY$;

BEGIN;
DROP TRIGGER IF EXISTS trigger_sync_namespace
ON namespace;
CREATE TRIGGER trigger_sync_namespace AFTER INSERT OR UPDATE OR DELETE ON namespace
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_namespace();
COMMIT;

CREATE OR REPLACE FUNCTION _sysinternal.sync_namespace_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on namespace_node table'
        USING ERRCODE = 'IO101';
    END IF;

    FOR schema_name IN
    SELECT n.schema_name
    FROM node AS n
    LOOP
        EXECUTE format(
            $$
                INSERT INTO %I.namespace_node SELECT $1.*
            $$,
            schema_name
        )
        USING NEW;
    END LOOP;
    RETURN NEW;
END
$BODY$;

BEGIN;
DROP TRIGGER IF EXISTS trigger_sync_namespace_node
ON namespace_node;
CREATE TRIGGER trigger_sync_namespace_node AFTER INSERT OR UPDATE OR DELETE ON namespace_node
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.sync_namespace_node();
COMMIT;
