-- Trigger to create/drop indexes on chunk tables when the corresponding
-- chunk_index row is created/deleted.
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_chunk_index()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'INSERT' THEN
        EXECUTE NEW.definition;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        EXECUTE format('DROP INDEX IF EXISTS %I.%I', OLD.schema_name, OLD.index_name);
        RETURN OLD;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
