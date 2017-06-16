-- Trigger for when chunk rows are changed.
-- On Insert: create chunk table, add indexes, add constraints.
-- On Update: change constraints.
-- On Delete: drop table
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    kind                  pg_class.relkind%type;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM _timescaledb_internal.chunk_create_table(NEW.id);

        PERFORM _timescaledb_internal.create_chunk_index_row(NEW.schema_name, NEW.table_name,
                                hi.main_schema_name, hi.main_index_name, hi.definition)
        FROM _timescaledb_catalog.hypertable_index hi
        WHERE hi.hypertable_id = NEW.hypertable_id;

        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        -- when deleting the chunk row from the metadata table,
        -- also DROP the actual chunk table that holds data.
        -- Note that the table could already be deleted in case this
        -- trigger fires as a result of a DROP TABLE on the hypertable
        -- that this chunk belongs to.

        EXECUTE format(
                $$
                SELECT c.relkind FROM pg_class c WHERE relname = '%I' AND relnamespace = '%I'::regnamespace
                $$, OLD.table_name, OLD.schema_name
        ) INTO kind;

        IF kind IS NULL THEN
            RETURN OLD;
        END IF;

        EXECUTE format(
            $$
            DROP TABLE %I.%I
            $$, OLD.schema_name, OLD.table_name
        );
        RETURN OLD;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
