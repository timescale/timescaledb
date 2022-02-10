-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains functions associated with creating new
-- hypertables.

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_is_finite(
    val      BIGINT
)
    RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    --end values of bigint reserved for infinite
    SELECT val > (-9223372036854775808)::bigint AND val < 9223372036854775807::bigint
$BODY$ SET search_path TO pg_catalog;


CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql(
    dimension_slice_id  INTEGER
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    dimension_slice_row _timescaledb_catalog.dimension_slice;
    dimension_row _timescaledb_catalog.dimension;
    dimension_def TEXT;
    dimtype REGTYPE;
    parts TEXT[];
BEGIN
    SELECT * INTO STRICT dimension_slice_row
    FROM _timescaledb_catalog.dimension_slice
    WHERE id = dimension_slice_id;

    SELECT * INTO STRICT dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE id = dimension_slice_row.dimension_id;

    IF dimension_row.partitioning_func_schema IS NOT NULL AND
       dimension_row.partitioning_func IS NOT NULL THEN
        SELECT prorettype INTO STRICT dimtype
        FROM pg_catalog.pg_proc pro
        WHERE pro.oid = format('%I.%I', dimension_row.partitioning_func_schema, dimension_row.partitioning_func)::regproc::oid;

        dimension_def := format('%1$I.%2$I(%3$I)',
             dimension_row.partitioning_func_schema,
             dimension_row.partitioning_func,
             dimension_row.column_name);
    ELSE
        dimension_def := format('%1$I', dimension_row.column_name);
        dimtype := dimension_row.column_type;
    END IF;

    IF dimension_row.num_slices IS NOT NULL THEN

        IF  _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(' %1$s >= %2$L ', dimension_def, dimension_slice_row.range_start);
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(' %1$s < %2$L ', dimension_def, dimension_slice_row.range_end);
        END IF;

        IF array_length(parts, 1) = 0 THEN
            RETURN NULL;
        END IF;
        return array_to_string(parts, 'AND');
    ELSE
        -- only works with time for now
        IF _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimtype) =
           _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimtype) THEN
            RAISE 'time-based constraints have the same start and end values for column "%": %',
                    dimension_row.column_name,
                    _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimtype);
        END IF;

        parts = ARRAY[]::text[];

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(' %1$s >= %2$s ',
            dimension_def,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimtype));
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(' %1$s < %2$s ',
            dimension_def,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimtype));
        END IF;

        return array_to_string(parts, 'AND');
    END IF;
END
$BODY$ SET search_path TO pg_catalog;

-- Outputs the create_hypertable command to recreate the given hypertable.
--
-- This is currently used internally for our single hypertable backup tool
-- so that it knows how to restore the hypertable without user intervention.
--
-- It only works for hypertables with up to 2 dimensions.
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_create_command(
    table_name NAME
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    h_id             INTEGER;
    schema_name      NAME;
    time_column      NAME;
    time_interval    BIGINT;
    space_column     NAME;
    space_partitions INTEGER;
    dimension_cnt    INTEGER;
    dimension_row    record;
    ret              TEXT;
BEGIN
    SELECT h.id, h.schema_name
    FROM _timescaledb_catalog.hypertable AS h
    WHERE h.table_name = get_create_command.table_name
    INTO h_id, schema_name;

    IF h_id IS NULL THEN
        RAISE EXCEPTION 'hypertable "%" not found', table_name
        USING ERRCODE = 'TS101';
    END IF;

    SELECT COUNT(*)
    FROM _timescaledb_catalog.dimension d
    WHERE d.hypertable_id = h_id
    INTO STRICT dimension_cnt;

    IF dimension_cnt > 2 THEN
        RAISE EXCEPTION 'get_create_command only supports hypertables with up to 2 dimensions'
        USING ERRCODE = 'TS101';
    END IF;

    FOR dimension_row IN
        SELECT *
        FROM _timescaledb_catalog.dimension d
        WHERE d.hypertable_id = h_id
        LOOP
        IF dimension_row.interval_length IS NOT NULL THEN
            time_column := dimension_row.column_name;
            time_interval := dimension_row.interval_length;
        ELSIF dimension_row.num_slices IS NOT NULL THEN
            space_column := dimension_row.column_name;
            space_partitions := dimension_row.num_slices;
        END IF;
    END LOOP;

    ret := format($$SELECT create_hypertable('%I.%I', '%s'$$, schema_name, table_name, time_column);
    IF space_column IS NOT NULL THEN
        ret := ret || format($$, '%I', %s$$, space_column, space_partitions);
    END IF;
    ret := ret || format($$, chunk_time_interval => %s, create_default_indexes=>FALSE);$$, time_interval);

    RETURN ret;
END
$BODY$ SET search_path TO pg_catalog;
