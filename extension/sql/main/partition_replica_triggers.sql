CREATE OR REPLACE FUNCTION _sysinternal.on_create_partition_replica_table()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    PERFORM _sysinternal.create_data_partition_table(
        NEW.schema_name, NEW.table_name,
        hr.schema_name, hr.table_name,
        p.keyspace_start, p.keyspace_end,
        p.epoch_id)
    FROM hypertable_replica hr
    CROSS JOIN partition p
    WHERE hr.hypertable_name = NEW.hypertable_name AND
          hr.replica_id = NEW.replica_id AND
          p.id = NEW.partition_id;

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';
