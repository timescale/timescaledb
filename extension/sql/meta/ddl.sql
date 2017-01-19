CREATE SEQUENCE IF NOT EXISTS default_hypertable_seq;

-- Creates a hypertable.
CREATE OR REPLACE FUNCTION _meta.create_hypertable(
    main_schema_name        NAME,
    main_table_name         NAME,
    time_column_name        NAME,
    time_column_type        REGTYPE,
    partitioning_column     NAME,
    replication_factor      SMALLINT,
    number_partitions       SMALLINT,
    associated_schema_name  NAME,
    associated_table_prefix NAME,
    hypertable_name         NAME,
    placement               chunk_placement_type,
    chunk_size_bytes        BIGINT,
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
        associated_schema_name = '_sysinternal';
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
        chunk_size_bytes,
        time_column_name, time_column_type,
        created_on)
    VALUES (
        hypertable_name,
        main_schema_name, main_table_name,
        associated_schema_name, associated_table_prefix,
        associated_schema_name, format('%s_root', associated_table_prefix),
        associated_schema_name, format('%s_distinct', associated_table_prefix),
        replication_factor,
        placement,
        chunk_size_bytes,
        time_column_name, time_column_type,
        created_on
      )
    RETURNING * INTO hypertable_row;

    IF number_partitions != 0 THEN
        PERFORM add_equi_partition_epoch(hypertable_name, number_partitions, partitioning_column);
    END IF;
    RETURN hypertable_row;
END
$BODY$;

-- Adds a column to a hypertable
CREATE OR REPLACE FUNCTION _meta.add_column(
    hypertable_name NAME,
    column_name     NAME,
    attnum          INT2,
    data_type       REGTYPE,
    default_value   TEXT,
    not_null        BOOLEAN,
    is_distinct     BOOLEAN,
    created_on      NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT 1 FROM hypertable h WHERE h.NAME = hypertable_name FOR UPDATE; --lock row to prevent concurrent column inserts; keep attnum consistent.

INSERT INTO hypertable_column (hypertable_name, name, attnum, data_type, default_value, not_null, is_distinct, created_on, modified_on)
VALUES (hypertable_name, column_name, attnum, data_type, default_value, not_null, is_distinct, created_on, created_on)
ON CONFLICT DO NOTHING;
$BODY$;

-- Drops a column from a hypertable
CREATE OR REPLACE FUNCTION _meta.drop_column(
    hypertable_name NAME,
    column_name     NAME,
    modified_on     NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT set_config('io.deleting_node', modified_on, true);
DELETE FROM hypertable_column c
WHERE c.hypertable_name = drop_column.hypertable_name AND c.NAME = column_name;
$BODY$;

-- Sets the is_distinct flag for a column on a hypertable.
CREATE OR REPLACE FUNCTION _meta.alter_column_set_is_distinct(
    hypertable_name   NAME,
    column_name       NAME,
    new_is_distinct   BOOLEAN,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE hypertable_column
SET is_distinct = new_is_distinct, modified_on = modified_on_node
WHERE hypertable_name = alter_column_set_is_distinct.hypertable_name AND name = column_name;
$BODY$;

-- Sets the default for a column on a hypertable.
CREATE OR REPLACE FUNCTION _meta.alter_column_set_default(
    hypertable_name   NAME,
    column_name       NAME,
    new_default_value TEXT,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE hypertable_column
SET default_value = new_default_value, modified_on = modified_on_node
WHERE hypertable_name = alter_column_set_default.hypertable_name AND name = column_name;
$BODY$;

-- Sets the not null flag for a column on a hypertable.
CREATE OR REPLACE FUNCTION _meta.alter_column_set_not_null(
    hypertable_name   NAME,
    column_name       NAME,
    new_not_null      BOOLEAN,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE hypertable_column
SET not_null = new_not_null, modified_on = modified_on_node
WHERE hypertable_name = alter_column_set_not_null.hypertable_name AND name = column_name;
$BODY$;

-- Renames the column on a hypertable
CREATE OR REPLACE FUNCTION _meta.alter_table_rename_column(
    hypertable_name   NAME,
    old_column_name    NAME,
    new_column_name    NAME,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE hypertable_column
SET NAME = new_column_name, modified_on = modified_on_node
WHERE hypertable_name = alter_table_rename_column.hypertable_name AND name = old_column_name;
$BODY$;

-- Add an index to a hypertable
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

-- Drops the index for a hypertable
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

-- Drops a hypertable
CREATE OR REPLACE FUNCTION _meta.drop_hypertable(
    schema_name NAME,
    table_name NAME,
    modified_on NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    SELECT set_config('io.deleting_node', modified_on, true);
    DELETE FROM hypertable h WHERE h.main_schema_name = schema_name AND h.main_table_name = table_name
$BODY$;
