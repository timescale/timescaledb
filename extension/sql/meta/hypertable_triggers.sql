-- Trigger on the meta node for when a new hypertable is added.
CREATE OR REPLACE FUNCTION _meta.on_create_hypertable()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on hypertable name '
        USING ERRCODE = 'IO101';
    END IF;

    INSERT INTO hypertable_replica
        SELECT
            NEW.name,
            replica_id,
            NEW.associated_schema_name,
            format('%s_%s_replica', NEW.associated_table_prefix, replica_id),
            NEW.associated_schema_name,
            format('%s_%s_distinct', NEW.associated_table_prefix, replica_id)
        FROM generate_series(0, NEW.replication_factor - 1) AS replica_id
    ON CONFLICT DO NOTHING;

    PERFORM _meta.assign_default_replica_node(n.database_name, NEW.name)
    FROM node n;

    RETURN NEW;
END
$BODY$;
