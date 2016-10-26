CREATE OR REPLACE FUNCTION on_create_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on node table';
    END IF;
    EXECUTE format(
        $$
            CREATE SCHEMA IF NOT EXISTS %I;
        $$,
        NEW.schema_name);

    PERFORM _create_server(NEW);

    PERFORM _create_user_mapping(cluster_user, NEW)
    FROM cluster_user;

    EXECUTE format(
        $$
            IMPORT FOREIGN SCHEMA public
            FROM SERVER %I
            INTO %I;
        $$,
        NEW.server_name, NEW.schema_name);
    RETURN NEW;
END
$BODY$;

BEGIN;
DROP TRIGGER IF EXISTS trigger_create_node
ON node;
CREATE TRIGGER trigger_create_node BEFORE INSERT OR UPDATE OR DELETE ON node
FOR EACH ROW EXECUTE PROCEDURE on_create_node();
COMMIT;

CREATE OR REPLACE FUNCTION sync_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    EXECUTE format(
        $$
            INSERT INTO %I.node SELECT * from node;
        $$,
        NEW.schema_name);

    FOR schema_name IN
    SELECT n.schema_name
    FROM node AS n
    WHERE n.schema_name <> NEW.schema_name
    LOOP
        EXECUTE format(
            $$
                INSERT INTO %I.node SELECT $1.*
            $$,
            schema_name
        )
        USING NEW;
    END LOOP;
    RETURN NEW;
END
$BODY$;

BEGIN;
DROP TRIGGER IF EXISTS trigger_sync_node
ON node;
CREATE TRIGGER trigger_sync_node AFTER INSERT ON node
FOR EACH ROW EXECUTE PROCEDURE sync_node();
COMMIT;
