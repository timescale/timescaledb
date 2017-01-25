CREATE OR REPLACE FUNCTION _iobeamdb_internal.on_create_partition_replica_table()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM _iobeamdb_internal.create_data_partition_table(
            NEW.schema_name, NEW.table_name,
            hr.schema_name, hr.table_name,
            p.keyspace_start, p.keyspace_end,
            p.epoch_id)
        FROM _iobeamdb_catalog.hypertable_replica hr
        CROSS JOIN _iobeamdb_catalog.partition p
        WHERE hr.hypertable_id = NEW.hypertable_id AND
            hr.replica_id = NEW.replica_id AND
            p.id = NEW.partition_id;

        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RAISE EXCEPTION 'Only inserts and deletes supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';

END
$BODY$
SET SEARCH_PATH = 'public';
