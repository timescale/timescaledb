CREATE OR REPLACE FUNCTION _timescaledb_meta.on_change_partition()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF current_setting('timescaledb_internal.originating_node') = 'on' THEN
            INSERT INTO _timescaledb_catalog.partition_replica (partition_id, hypertable_id, replica_id, schema_name, table_name)
                (SELECT
                    NEW.id,
                    hypertable_id,
                    replica_id,
                    h.associated_schema_name,
                    format('%s_%s_%s_partition', h.associated_table_prefix, NEW.id, replica_id)
                FROM _timescaledb_catalog.hypertable_replica hr
                INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = hr.hypertable_id)
                WHERE hypertable_id = (
                    SELECT hypertable_id
                    FROM _timescaledb_catalog.partition_epoch
                    WHERE id = NEW.epoch_id
                ));
        END IF;

        RETURN NEW;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
