CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'UPDATE' THEN
        PERFORM _timescaledb_internal.set_time_constraint(crn.schema_name, crn.table_name, NEW.start_time, NEW.end_time)
        FROM _timescaledb_catalog.chunk_replica_node crn
        WHERE crn.chunk_id = NEW.id;
        RETURN NEW;
    END IF;
    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
