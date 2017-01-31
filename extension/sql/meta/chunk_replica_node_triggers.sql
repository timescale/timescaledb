CREATE OR REPLACE FUNCTION _iobeamdb_meta.on_change_chunk_replica_node_meta()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;

    PERFORM _iobeamdb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
