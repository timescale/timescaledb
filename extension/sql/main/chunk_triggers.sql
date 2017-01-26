-- TODO(rrk) - Why have this trigger for INSERTs/DELETEs? Nothing is actually done.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.on_change_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP <> 'INSERT' AND TG_OP <> 'UPDATE' AND TG_OP <> 'DELETE' THEN
        RAISE EXCEPTION 'Only inserts, updates, and deletes supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    IF TG_OP = 'UPDATE' THEN
        PERFORM _iobeamdb_internal.set_time_constraint(crn.schema_name, crn.table_name, NEW.start_time, NEW.end_time)
        FROM _iobeamdb_catalog.chunk_replica_node crn
        WHERE crn.chunk_id = NEW.id;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';
