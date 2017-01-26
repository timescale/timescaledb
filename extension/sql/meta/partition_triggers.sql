CREATE OR REPLACE FUNCTION _iobeamdb_meta.on_create_partition()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP = 'INSERT' THEN

    INSERT INTO _iobeamdb_catalog.partition_replica (partition_id, hypertable_id, replica_id, schema_name, table_name)
        SELECT
            NEW.id,
            hypertable_id,
            replica_id,
            h.associated_schema_name,
            format('%s_%s_%s_partition', h.associated_table_prefix, NEW.id, replica_id)
        FROM _iobeamdb_catalog.hypertable_replica hr
        INNER JOIN _iobeamdb_catalog.hypertable h ON (h.id = hr.hypertable_id)
        WHERE hypertable_id = (
            SELECT hypertable_id
            FROM _iobeamdb_catalog.partition_epoch
            WHERE id = NEW.epoch_id
        );

    RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RAISE EXCEPTION 'Only inserts and deletes supported on % name ', TG_TABLE_NAME
         USING ERRCODE = 'IO101';

END
$BODY$;
