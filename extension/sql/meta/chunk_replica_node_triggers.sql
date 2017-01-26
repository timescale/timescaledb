CREATE OR REPLACE FUNCTION _iobeamdb_meta.on_change_chunk_replica_node_meta()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO _iobeamdb_catalog.distinct_replica_node (hypertable_id, replica_id, database_name, schema_name, table_name)
            (SELECT
                pr.hypertable_id,
                pr.replica_id,
                NEW.database_name,
                h.associated_schema_name,
                format('%s_%s_%s_distinct_data', h.associated_table_prefix, pr.replica_id, n.id)
            FROM _iobeamdb_catalog.partition_replica pr
            INNER JOIN _iobeamdb_catalog.hypertable h ON (h.id = pr.hypertable_id)
            INNER JOIN _iobeamdb_catalog.node n ON (n.database_name = NEW.database_name)
            WHERE pr.id = NEW.partition_replica_id)
        ON CONFLICT DO NOTHING;

        RETURN NEW;
    END IF;

    PERFORM _iobeamdb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
