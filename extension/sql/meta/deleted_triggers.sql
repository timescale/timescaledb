-- Trigger to track columns and indices that are deleted on hypertables.
CREATE OR REPLACE FUNCTION _meta.log_delete()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    EXECUTE format(
        $$
            INSERT INTO %I.%I SELECT $1.*, %L
        $$,
        TG_TABLE_SCHEMA,
        format('deleted_%s', TG_TABLE_NAME),
        current_setting('io.deleting_node', false)
    )
    USING OLD;
    RETURN OLD;
END
$BODY$;
