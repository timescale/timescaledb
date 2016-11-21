CREATE OR REPLACE FUNCTION _sysinternal.create_chunk_replica_node_indexes_for_field(
    hypertable_name NAME,
    field_name      NAME,
    index_types     field_index_type []
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    PERFORM _sysinternal.create_chunk_replica_node_index(crn.schema_name, crn.table_name, field_name, index_type)
    FROM chunk_replica_node crn
    INNER JOIN partition_replica pr ON (pr.id = crn.partition_replica_id)
    CROSS JOIN unnest(index_types) AS index_type
    WHERE pr.hypertable_name = create_chunk_replica_node_indexes_for_field.hypertable_name;
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.create_partition_constraint_for_field(
    hypertable_name NAME,
    field_name      NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM _sysinternal.add_partition_constraint(pr.schema_name, pr.table_name, p.keyspace_start, p.keyspace_end,
                                                  p.epoch_id)
    FROM partition_epoch pe
    INNER JOIN partition p ON (p.epoch_id = pe.id)
    INNER JOIN partition_replica pr ON (pr.partition_id = p.id)
    WHERE pe.hypertable_name = create_partition_constraint_for_field.hypertable_name
          AND pe.partitioning_field = create_partition_constraint_for_field.field_name;
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.create_field_on_root_table(
    schema_name   NAME,
    table_name    NAME,
    field         NAME,
    data_type_oid REGTYPE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    EXECUTE format(
        $$
            ALTER TABLE %1$I.%2$I ADD COLUMN %3$I %4$s DEFAULT NULL
        $$,
        schema_name, table_name, field, data_type_oid);

END
$BODY$;


CREATE OR REPLACE FUNCTION _sysinternal.on_create_field()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    hypertable_row hypertable;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    SELECT *
    INTO STRICT hypertable_row
    FROM hypertable AS h
    WHERE h.name = NEW.hypertable_name;


    PERFORM _sysinternal.create_field_on_root_table(hypertable_row.root_schema_name, hypertable_row.root_table_name,
                                                    NEW.name, NEW.data_type);
    PERFORM _sysinternal.create_partition_constraint_for_field(NEW.hypertable_name, NEW.name);
    PERFORM _sysinternal.create_chunk_replica_node_indexes_for_field(NEW.hypertable_name, NEW.name, NEW.index_types);
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_field
ON field;
CREATE TRIGGER trigger_on_create_field AFTER INSERT OR UPDATE OR DELETE ON field
FOR EACH ROW EXECUTE PROCEDURE _sysinternal.on_create_field();
COMMIT;
