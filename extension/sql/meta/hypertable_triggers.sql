-- Trigger on the meta node for when a new hypertable is added.
CREATE OR REPLACE FUNCTION _iobeamdb_meta.on_change_hypertable()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO _iobeamdb_catalog.hypertable_replica
        SELECT
            NEW.id,
            replica_id,
            NEW.associated_schema_name,
            format('%s_%s_replica', NEW.associated_table_prefix, replica_id)
        FROM generate_series(0, NEW.replication_factor - 1) AS replica_id;

        PERFORM _iobeamdb_meta.assign_default_replica_node(n.database_name, NEW.id)
        FROM _iobeamdb_catalog.node n;
        RETURN NEW;
    END IF;

    PERFORM _iobeamdb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
