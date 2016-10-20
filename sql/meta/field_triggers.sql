CREATE OR REPLACE FUNCTION sync_field()
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
                INSERT INTO %I.field SELECT $1.*
            $$,
            schema_name
        )
        USING NEW;
    END LOOP;
    RETURN NEW;
END
$BODY$;

BEGIN;
DROP TRIGGER IF EXISTS trigger_sync_field
ON field;
CREATE TRIGGER trigger_sync_field AFTER INSERT OR UPDATE OR DELETE ON field
FOR EACH ROW EXECUTE PROCEDURE sync_field();
COMMIT;