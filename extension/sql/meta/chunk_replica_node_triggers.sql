CREATE OR REPLACE FUNCTION _iobeamdb_meta.on_create_chunk_replica_node_meta()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'INSERT' THEN
    INSERT INTO _iobeamdb_catalog.distinct_replica_node (hypertable_name, replica_id, database_name, schema_name, table_name)
        SELECT
            pr.hypertable_name,
            pr.replica_id,
            NEW.database_name,
            h.associated_schema_name,
            format('%s_%s_%s_distinct_data', h.associated_table_prefix, pr.replica_id, n.id)
        FROM _iobeamdb_catalog.partition_replica pr
        INNER JOIN _iobeamdb_catalog.hypertable h ON (h.name = pr.hypertable_name)
        INNER JOIN _iobeamdb_catalog.node n ON (n.database_name = NEW.database_name)
        WHERE pr.id = NEW.partition_replica_id
    ON CONFLICT DO NOTHING;

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
