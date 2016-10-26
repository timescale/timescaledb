CREATE OR REPLACE FUNCTION on_create_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    cluster_user_row cluster_user;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on node table';
    END IF;

    IF NEW.database_name <> current_database() THEN
        PERFORM _create_server(NEW);

        FOR cluster_user_row IN SELECT *
                                FROM cluster_user LOOP
            PERFORM _create_user_mapping(cluster_user_row, NEW);
        END LOOP;
    END IF;
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_node
ON node;
CREATE TRIGGER trigger_on_create_node AFTER INSERT OR UPDATE OR DELETE ON node
FOR EACH ROW EXECUTE PROCEDURE on_create_node();
COMMIT;