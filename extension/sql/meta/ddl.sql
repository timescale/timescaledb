CREATE SEQUENCE IF NOT EXISTS default_hypertable_seq;

/*
  Creates a hypertable.
*/
CREATE OR REPLACE FUNCTION _meta.add_hypertable(
    main_schema_name        NAME,
    main_table_name         NAME,
    time_field_name         NAME,
    time_field_type         REGTYPE,
    partitioning_field      NAME,
    replication_factor      SMALLINT,
    number_partitions       SMALLINT,
    associated_schema_name  NAME,
    associated_table_prefix NAME,
    hypertable_name         NAME,
    placement               chunk_placement_type,
    created_on              NAME
)
    RETURNS hypertable LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    id INTEGER;
    hypertable_row hypertable;
BEGIN

    id :=  nextval('default_hypertable_seq');

    IF associated_schema_name IS NULL THEN
        associated_schema_name = format('_sys_%s_%s', id, hypertable_name);
    END IF;

    IF associated_table_prefix IS NULL THEN
        associated_table_prefix = format('_hyper_%s', id);
    END IF;

    IF number_partitions IS NULL THEN
        SELECT COUNT(*)
        INTO number_partitions
        FROM node;
    END IF;

    IF hypertable_name IS NULL THEN
      hypertable_name = format('%I.%I', main_schema_name, main_table_name);
    END IF;

    INSERT INTO hypertable (
        name,
        main_schema_name, main_table_name,
        associated_schema_name, associated_table_prefix,
        root_schema_name, root_table_name,
        distinct_schema_name, distinct_table_name,
        replication_factor,
        placement, 
        time_field_name, time_field_type,
        created_on)
    VALUES (
        hypertable_name,
        main_schema_name, main_table_name,
        associated_schema_name, associated_table_prefix,
        associated_schema_name, format('%s_root', associated_table_prefix),
        associated_schema_name, format('%s_distinct', associated_table_prefix),
        replication_factor, 
        placement,
        time_field_name, time_field_type,
        created_on
      )
    ON CONFLICT DO NOTHING RETURNING * INTO hypertable_row;

    IF number_partitions != 0 THEN
        PERFORM add_equi_partition_epoch(hypertable_name, number_partitions, partitioning_field);
    END IF;
    RETURN hypertable_row;
END
$BODY$;

--Adds a column to a hypertable
CREATE OR REPLACE FUNCTION _meta.add_field(
    hypertable_name NAME,
    field_name      NAME,
    attnum          INT2,
    data_type       REGTYPE,
    default_value   TEXT,
    not_null        BOOLEAN,
    is_distinct     BOOLEAN,
    created_on      NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT 1 FROM hypertable h WHERE h.NAME = hypertable_name FOR UPDATE; --lock row to prevent concurrent field inserts; keep attnum consistent.

INSERT INTO field (hypertable_name, name, attnum, data_type, default_value, not_null, is_distinct, created_on, modified_on)
VALUES (hypertable_name, field_name, attnum, data_type, default_value, not_null, is_distinct, created_on, created_on)
ON CONFLICT DO NOTHING;
$BODY$;

--Drps a colum from a hypertable
CREATE OR REPLACE FUNCTION _meta.drop_field(
    hypertable_name NAME,
    field_name      NAME,
    modified_on      NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT set_config('io.deleting_node', modified_on, true);
DELETE FROM field f 
WHERE f.hypertable_name = drop_field.hypertable_name AND f.NAME = field_name; 
$BODY$;

--Sets the is_distinct flag for a field on a hypertable.
CREATE OR REPLACE FUNCTION _meta.alter_column_set_is_distinct(
    hypertable_name   NAME,
    field_name        NAME,
    new_is_distinct   BOOLEAN,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE field 
SET is_distinct = new_is_distinct, modified_on = modified_on_node 
WHERE hypertable_name = alter_column_set_is_distinct.hypertable_name AND name = field_name; 
$BODY$;

--Sets the default for a column on a hypertable.
CREATE OR REPLACE FUNCTION _meta.alter_column_set_default(
    hypertable_name   NAME,
    field_name        NAME,
    new_default_value TEXT,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE field 
SET default_value = new_default_value, modified_on = modified_on_node 
WHERE hypertable_name = alter_column_set_default.hypertable_name AND name = field_name; 
$BODY$;

--Sets the not null flag for a column on a hypertable.
CREATE OR REPLACE FUNCTION _meta.alter_column_set_not_null(
    hypertable_name   NAME,
    field_name        NAME,
    new_not_null      BOOLEAN,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE field 
SET not_null = new_not_null, modified_on = modified_on_node 
WHERE hypertable_name = alter_column_set_not_null.hypertable_name AND name = field_name; 
$BODY$;

--Renames the column on a hypertable
CREATE OR REPLACE FUNCTION _meta.alter_table_rename_column(
    hypertable_name   NAME,
    old_field_name    NAME,
    new_field_name    NAME,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE field 
SET NAME = new_field_name, modified_on = modified_on_node 
WHERE hypertable_name = alter_table_rename_column.hypertable_name AND name = old_field_name; 
$BODY$;

--Add an index to a hypertable
CREATE OR REPLACE FUNCTION _meta.add_index(
    hypertable_name NAME,
    main_schema_name NAME,
    main_index_name NAME,
    definition TEXT,
    created_on NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
INSERT INTO hypertable_index (hypertable_name, main_schema_name, main_index_name, definition, created_on)
VALUES (hypertable_name, main_schema_name, main_index_name, definition, created_on)
ON CONFLICT DO NOTHING;
$BODY$;

--Drops the index for a hypertable
CREATE OR REPLACE FUNCTION _meta.drop_index(
    main_schema_name NAME, 
    main_index_name NAME,
    modified_on NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT set_config('io.deleting_node', modified_on, true);
DELETE FROM hypertable_index i
WHERE i.main_index_name = drop_index.main_index_name AND i.main_schema_name = drop_index.main_schema_name; 
$BODY$;

