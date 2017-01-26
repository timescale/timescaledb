CREATE SEQUENCE IF NOT EXISTS _iobeamdb_catalog.default_hypertable_seq;
SELECT pg_catalog.pg_extension_config_dump('_iobeamdb_catalog.default_hypertable_seq', '');

-- Creates a hypertable.
CREATE OR REPLACE FUNCTION _iobeamdb_meta.create_hypertable(
    schema_name             NAME,
    table_name              NAME,
    time_column_name        NAME,
    time_column_type        REGTYPE,
    partitioning_column     NAME,
    replication_factor      SMALLINT,
    number_partitions       SMALLINT,
    associated_schema_name  NAME,
    associated_table_prefix NAME,
    placement               _iobeamdb_catalog.chunk_placement_type,
    chunk_size_bytes        BIGINT,
    created_on              NAME
)
    RETURNS _iobeamdb_catalog.hypertable LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    id                       INTEGER;
    hypertable_row           _iobeamdb_catalog.hypertable;
    partitioning_func        _iobeamdb_catalog.partition_epoch.partitioning_func%TYPE = 'get_partition_for_key';
    partitioning_func_schema _iobeamdb_catalog.partition_epoch.partitioning_func_schema%TYPE = '_iobeamdb_catalog';
BEGIN

    id :=  nextval('_iobeamdb_catalog.default_hypertable_seq');

    IF associated_schema_name IS NULL THEN
        associated_schema_name = '_iobeamdb_internal';
    END IF;

    IF associated_table_prefix IS NULL THEN
        associated_table_prefix = format('_hyper_%s', id);
    END IF;

    IF partitioning_column IS NULL THEN
        IF number_partitions IS NULL THEN
            number_partitions := 1;
            partitioning_func := NULL;
            partitioning_func_schema := NULL;
        ELSIF number_partitions <> 1 THEN
            RAISE EXCEPTION 'The number of partitions must be 1 without a partitioning column'
            USING ERRCODE ='IO101';
        END IF;
    ELSIF number_partitions IS NULL THEN
        SELECT COUNT(*)
        INTO number_partitions
        FROM _iobeamdb_catalog.node;
    END IF;

    INSERT INTO _iobeamdb_catalog.hypertable (
        schema_name, table_name,
        associated_schema_name, associated_table_prefix,
        root_schema_name, root_table_name,
        distinct_schema_name, distinct_table_name,
        replication_factor,
        placement,
        chunk_size_bytes,
        time_column_name, time_column_type,
        created_on)
    VALUES (
        schema_name, table_name,
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
        PERFORM add_equi_partition_epoch(hypertable_row.id, number_partitions, partitioning_column, partitioning_func_schema, partitioning_func);
    END IF;
    RETURN hypertable_row;
END
$BODY$;

-- Adds a column to a hypertable
CREATE OR REPLACE FUNCTION _iobeamdb_meta.add_column(
    hypertable_id INTEGER,
    column_name   NAME,
    attnum        INT2,
    data_type     REGTYPE,
    default_value TEXT,
    not_null      BOOLEAN,
    is_distinct   BOOLEAN,
    created_on    NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT 1 FROM _iobeamdb_catalog.hypertable h WHERE h.id = hypertable_id FOR UPDATE; --lock row to prevent concurrent column inserts; keep attnum consistent.

INSERT INTO _iobeamdb_catalog.hypertable_column (hypertable_id, name, attnum, data_type, default_value, not_null, is_distinct, created_on, modified_on)
VALUES (hypertable_id, column_name, attnum, data_type, default_value, not_null, is_distinct, created_on, created_on);
$BODY$;

-- Drops a column from a hypertable
CREATE OR REPLACE FUNCTION _iobeamdb_meta.drop_column(
    hypertable_id INTEGER,
    column_name   NAME,
    modified_on   NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT set_config('io.deleting_node', modified_on, true);
DELETE FROM _iobeamdb_catalog.hypertable_column c
WHERE c.hypertable_id = drop_column.hypertable_id AND c.NAME = column_name;
$BODY$;

-- Sets the is_distinct flag for a column on a hypertable.
CREATE OR REPLACE FUNCTION _iobeamdb_meta.alter_column_set_is_distinct(
    hypertable_id     INTEGER,
    column_name       NAME,
    new_is_distinct   BOOLEAN,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE _iobeamdb_catalog.hypertable_column
SET is_distinct = new_is_distinct, modified_on = modified_on_node
WHERE hypertable_id = alter_column_set_is_distinct.hypertable_id AND name = column_name;
$BODY$;

-- Sets the default for a column on a hypertable.
CREATE OR REPLACE FUNCTION _iobeamdb_meta.alter_column_set_default(
    hypertable_id     INTEGER,
    column_name       NAME,
    new_default_value TEXT,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE _iobeamdb_catalog.hypertable_column
SET default_value = new_default_value, modified_on = modified_on_node
WHERE hypertable_id = alter_column_set_default.hypertable_id AND name = column_name;
$BODY$;

-- Sets the not null flag for a column on a hypertable.
CREATE OR REPLACE FUNCTION _iobeamdb_meta.alter_column_set_not_null(
    hypertable_id     INTEGER,
    column_name       NAME,
    new_not_null      BOOLEAN,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE _iobeamdb_catalog.hypertable_column
SET not_null = new_not_null, modified_on = modified_on_node
WHERE hypertable_id = alter_column_set_not_null.hypertable_id AND name = column_name;
$BODY$;

-- Renames the column on a hypertable
CREATE OR REPLACE FUNCTION _iobeamdb_meta.alter_table_rename_column(
    hypertable_id    INTEGER,
    old_column_name  NAME,
    new_column_name  NAME,
    modified_on_node NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE _iobeamdb_catalog.hypertable_column
SET NAME = new_column_name, modified_on = modified_on_node
WHERE hypertable_id = alter_table_rename_column.hypertable_id AND name = old_column_name;
$BODY$;

-- Add an index to a hypertable
CREATE OR REPLACE FUNCTION _iobeamdb_meta.add_index(
    hypertable_id    INTEGER,
    main_schema_name NAME,
    main_index_name  NAME,
    definition       TEXT,
    created_on       NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
INSERT INTO _iobeamdb_catalog.hypertable_index (hypertable_id, main_schema_name, main_index_name, definition, created_on)
VALUES (hypertable_id, main_schema_name, main_index_name, definition, created_on);
$BODY$;

-- Drops the index for a hypertable
CREATE OR REPLACE FUNCTION _iobeamdb_meta.drop_index(
    main_schema_name NAME,
    main_index_name  NAME,
    modified_on      NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT set_config('io.deleting_node', modified_on, true);
DELETE FROM _iobeamdb_catalog.hypertable_index i
WHERE i.main_index_name = drop_index.main_index_name AND i.main_schema_name = drop_index.main_schema_name;
$BODY$;

-- Drops a hypertable
CREATE OR REPLACE FUNCTION _iobeamdb_meta.drop_hypertable(
    schema_name NAME,
    table_name  NAME,
    modified_on NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    SELECT set_config('io.deleting_node', modified_on, true);
    DELETE FROM _iobeamdb_catalog.hypertable h
    WHERE h.schema_name = drop_hypertable.schema_name AND
          h.table_name = drop_hypertable.table_name
$BODY$;

-- Drop chunks older than the given timestamp. If a hypertable name is given,
-- drop only chunks associated with this table.
CREATE OR REPLACE FUNCTION _iobeamdb_meta.drop_chunks_older_than(
    older_than_time  BIGINT,
    table_name  NAME = NULL,
    schema_name NAME = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    EXECUTE format(
        $$
        DELETE FROM _iobeamdb_catalog.chunk c
        USING _iobeamdb_catalog.chunk_replica_node crn,
        _iobeamdb_catalog.partition_replica pr,
        _iobeamdb_catalog.hypertable h
        WHERE pr.id = crn.partition_replica_id
        AND pr.hypertable_id = h.id
        AND c.id = crn.chunk_id
        AND c.end_time < %1$L
        AND (%2$L IS NULL OR h.schema_name = %2$L)
        AND (%3$L IS NULL OR h.table_name = %3$L)
        $$, older_than_time, schema_name, table_name
    );
END
$BODY$;
