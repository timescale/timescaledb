CREATE SEQUENCE IF NOT EXISTS _timescaledb_catalog.default_hypertable_seq;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.default_hypertable_seq', '');

-- Creates a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_meta.create_hypertable(
    schema_name             NAME,
    table_name              NAME,
    time_column_name        NAME,
    time_column_type        REGTYPE,
    partitioning_column     NAME,
    number_partitions       INTEGER,
    replication_factor      SMALLINT,
    associated_schema_name  NAME,
    associated_table_prefix NAME,
    placement               _timescaledb_catalog.chunk_placement_type,
    chunk_time_interval        BIGINT,
    tablespace              NAME,
    created_on              NAME
)
    RETURNS _timescaledb_catalog.hypertable LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    id                       INTEGER;
    hypertable_row           _timescaledb_catalog.hypertable;
    partitioning_func        _timescaledb_catalog.partition_epoch.partitioning_func%TYPE = 'get_partition_for_key';
    partitioning_func_schema _timescaledb_catalog.partition_epoch.partitioning_func_schema%TYPE = '_timescaledb_catalog';
BEGIN

    id :=  nextval('_timescaledb_catalog.default_hypertable_seq');

    IF associated_schema_name IS NULL THEN
        associated_schema_name = '_timescaledb_internal';
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
        RAISE EXCEPTION 'The number of partitions must be specified when there is a partitioning column'
        USING ERRCODE ='IO101';
    END IF;

    IF number_partitions IS NOT NULL AND
       (number_partitions < 1 OR number_partitions > 32767) THEN
        RAISE EXCEPTION 'Invalid number of partitions'
        USING ERRCODE ='IO101';
    END IF;

    INSERT INTO _timescaledb_catalog.hypertable (
        schema_name, table_name,
        associated_schema_name, associated_table_prefix,
        root_schema_name, root_table_name,
        replication_factor,
        placement,
        chunk_time_interval,
        time_column_name, time_column_type,
        created_on)
    VALUES (
        schema_name, table_name,
        associated_schema_name, associated_table_prefix,
        associated_schema_name, format('%s_root', associated_table_prefix),
        replication_factor,
        placement,
        chunk_time_interval,
        time_column_name, time_column_type,
        created_on
      )
    RETURNING * INTO hypertable_row;

    IF number_partitions != 0 THEN
        PERFORM add_equi_partition_epoch(hypertable_row.id, number_partitions::smallint, partitioning_column,
                                         partitioning_func_schema, partitioning_func, tablespace);
    END IF;
    RETURN hypertable_row;
END
$BODY$;

-- Update chunk_time_interval for hypertable
CREATE OR REPLACE FUNCTION _timescaledb_meta.set_chunk_time_interval(
    schema_name NAME,
    table_name  NAME,
    time_interval  BIGINT,
    modified_on NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    UPDATE _timescaledb_catalog.hypertable h SET chunk_time_interval = time_interval
           WHERE h.schema_name = set_chunk_time_interval.schema_name AND
                 h.table_name = set_chunk_time_interval.table_name;
$BODY$;

-- Adds a column to a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_meta.add_column(
    hypertable_id INTEGER,
    column_name   NAME,
    attnum        INT2,
    data_type     REGTYPE,
    default_value TEXT,
    not_null      BOOLEAN,
    created_on    NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT 1 FROM _timescaledb_catalog.hypertable h WHERE h.id = hypertable_id FOR UPDATE; --lock row to prevent concurrent column inserts; keep attnum consistent.

INSERT INTO _timescaledb_catalog.hypertable_column (hypertable_id, name, attnum, data_type, default_value, not_null, created_on, modified_on)
VALUES (hypertable_id, column_name, attnum, data_type, default_value, not_null, created_on, created_on);
$BODY$;

-- Drops a column from a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_meta.drop_column(
    hypertable_id INTEGER,
    column_name   NAME,
    modified_on   NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT set_config('io.deleting_node', modified_on, true);
DELETE FROM _timescaledb_catalog.hypertable_column c
WHERE c.hypertable_id = drop_column.hypertable_id AND c.NAME = column_name;
$BODY$;

-- Sets the default for a column on a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_meta.alter_column_set_default(
    hypertable_id     INTEGER,
    column_name       NAME,
    new_default_value TEXT,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE _timescaledb_catalog.hypertable_column
SET default_value = new_default_value, modified_on = modified_on_node
WHERE hypertable_id = alter_column_set_default.hypertable_id AND name = column_name;
$BODY$;

-- Sets the not null flag for a column on a hypertable.
CREATE OR REPLACE FUNCTION _timescaledb_meta.alter_column_set_not_null(
    hypertable_id     INTEGER,
    column_name       NAME,
    new_not_null      BOOLEAN,
    modified_on_node  NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE _timescaledb_catalog.hypertable_column
SET not_null = new_not_null, modified_on = modified_on_node
WHERE hypertable_id = alter_column_set_not_null.hypertable_id AND name = column_name;
$BODY$;

-- Renames the column on a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_meta.alter_table_rename_column(
    hypertable_id    INTEGER,
    old_column_name  NAME,
    new_column_name  NAME,
    modified_on_node NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
UPDATE _timescaledb_catalog.hypertable_column
SET NAME = new_column_name, modified_on = modified_on_node
WHERE hypertable_id = alter_table_rename_column.hypertable_id AND name = old_column_name;
$BODY$;

-- Add an index to a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_meta.add_index(
    hypertable_id    INTEGER,
    main_schema_name NAME,
    main_index_name  NAME,
    definition       TEXT,
    created_on       NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
INSERT INTO _timescaledb_catalog.hypertable_index (hypertable_id, main_schema_name, main_index_name, definition, created_on)
VALUES (hypertable_id, main_schema_name, main_index_name, definition, created_on);
$BODY$;

-- Drops the index for a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_meta.drop_index(
    main_schema_name NAME,
    main_index_name  NAME,
    modified_on      NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
SELECT set_config('io.deleting_node', modified_on, true);
DELETE FROM _timescaledb_catalog.hypertable_index i
WHERE i.main_index_name = drop_index.main_index_name AND i.main_schema_name = drop_index.main_schema_name;
$BODY$;

-- Drops a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_meta.drop_hypertable(
    schema_name NAME,
    table_name  NAME,
    modified_on NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
    SELECT set_config('io.deleting_node', modified_on, true);
    DELETE FROM _timescaledb_catalog.hypertable h
    WHERE h.schema_name = drop_hypertable.schema_name AND
          h.table_name = drop_hypertable.table_name
$BODY$;

-- Drop chunks older than the given timestamp. If a hypertable name is given,
-- drop only chunks associated with this table.
CREATE OR REPLACE FUNCTION _timescaledb_meta.drop_chunks_older_than(
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
        DELETE FROM _timescaledb_catalog.chunk c
        USING _timescaledb_catalog.chunk_replica_node crn,
        _timescaledb_catalog.partition_replica pr,
        _timescaledb_catalog.hypertable h
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

CREATE OR REPLACE FUNCTION drop_chunks(
    older_than TIMESTAMPTZ,
    table_name  NAME = NULL,
    schema_name NAME = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    older_than_internal BIGINT;
BEGIN
    SELECT (EXTRACT(epoch FROM older_than)*1e6)::BIGINT INTO older_than_internal;
    PERFORM _timescaledb_meta.drop_chunks_older_than(older_than_internal, table_name, schema_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION drop_chunks(
    older_than  INTERVAL,
    table_name  NAME = NULL,
    schema_name NAME = NULL
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    older_than_ts TIMESTAMPTZ;
BEGIN
    older_than_ts := now() - older_than;
    PERFORM drop_chunks(older_than_ts, table_name, schema_name);
END
$BODY$;
