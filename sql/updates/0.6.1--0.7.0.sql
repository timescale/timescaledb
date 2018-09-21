DROP FUNCTION IF EXISTS drop_chunks(INTEGER, NAME, NAME, BOOLEAN);

DROP FUNCTION IF EXISTS _timescaledb_internal.create_chunk_constraint(integer,oid);
DROP FUNCTION IF EXISTS _timescaledb_internal.add_constraint(integer,oid);
DROP FUNCTION IF EXISTS _timescaledb_internal.add_constraint_by_name(integer,name);
DROP FUNCTION IF EXISTS _timescaledb_internal.need_chunk_constraint(oid);

INSERT INTO _timescaledb_catalog.chunk_index (chunk_id, index_name, hypertable_id, hypertable_index_name)
SELECT chunk_con.chunk_id, pg_chunk_index_class.relname, chunk.hypertable_id, pg_hypertable_index_class.relname
FROM _timescaledb_catalog.chunk_constraint chunk_con
INNER JOIN _timescaledb_catalog.chunk chunk ON (chunk_con.chunk_id = chunk.id)
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
INNER JOIN pg_constraint pg_chunk_con ON (
        pg_chunk_con.conrelid = format('%I.%I', chunk.schema_name, chunk.table_name)::regclass
        AND pg_chunk_con.conname = chunk_con.constraint_name
        AND pg_chunk_con.contype != 'f'
)
INNER JOIN pg_class pg_chunk_index_class ON (
    pg_chunk_con.conindid = pg_chunk_index_class.oid
)
INNER JOIN pg_constraint pg_hypertable_con ON (
        pg_hypertable_con.conrelid = format('%I.%I', hypertable.schema_name, hypertable.table_name)::regclass
        AND pg_hypertable_con.conname = chunk_con.hypertable_constraint_name
)
INNER JOIN pg_class pg_hypertable_index_class ON (
    pg_hypertable_con.conindid = pg_hypertable_index_class.oid
);

UPDATE _timescaledb_catalog.dimension_slice SET range_end = 9223372036854775807 WHERE range_end = 2147483647;
UPDATE _timescaledb_catalog.dimension_slice SET range_start = -9223372036854775808 WHERE range_start = 0;

DROP FUNCTION IF EXISTS _timescaledb_internal.range_value_to_pretty(BIGINT, regtype);

-- Upgrade support for setting partitioning function
DROP FUNCTION IF EXISTS create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean);
DROP FUNCTION IF EXISTS add_dimension(regclass,name,integer,bigint);
DROP FUNCTION IF EXISTS _timescaledb_internal.create_hypertable_row(regclass,name,name,name,name,integer,name,name,bigint,name);
DROP FUNCTION IF EXISTS _timescaledb_internal.add_dimension(regclass,_timescaledb_catalog.hypertable,name,integer,bigint,boolean);

--- Post script

CREATE OR REPLACE FUNCTION _timescaledb_internal.set_time_columns_not_null()
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
        ht_time_column RECORD;
BEGIN

        FOR ht_time_column IN
        SELECT ht.schema_name, ht.table_name, d.column_name
        FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.dimension d
        WHERE ht.id = d.hypertable_id AND d.partitioning_func IS NULL
        LOOP
                EXECUTE format(
                $$
                ALTER TABLE %I.%I ALTER %I SET NOT NULL
                $$, ht_time_column.schema_name, ht_time_column.table_name, ht_time_column.column_name);
        END LOOP;
END
$BODY$;

SELECT _timescaledb_internal.set_time_columns_not_null();

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_unix_microseconds(ts TIMESTAMPTZ) RETURNS BIGINT
    AS '@MODULE_PATHNAME@', 'ts_pg_timestamp_to_unix_microseconds' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.to_timestamp(unixtime_us BIGINT) RETURNS TIMESTAMPTZ
    AS '@MODULE_PATHNAME@', 'ts_pg_unix_microseconds_to_timestamp' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_is_finite(
    val      BIGINT
)
    RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    --end values of bigint reserved for infinite
    SELECT val > (-9223372036854775808)::bigint AND val < 9223372036854775807::bigint
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql(
    dimension_slice_id  INTEGER
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    dimension_slice_row _timescaledb_catalog.dimension_slice;
    dimension_row _timescaledb_catalog.dimension;
    parts TEXT[];
BEGIN
    SELECT * INTO STRICT dimension_slice_row
    FROM _timescaledb_catalog.dimension_slice
    WHERE id = dimension_slice_id;

    SELECT * INTO STRICT dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE id = dimension_slice_row.dimension_id;

    IF dimension_row.partitioning_func IS NOT NULL THEN

        IF  _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(
            $$
                %1$I.%2$I(%3$I) >= %4$L
            $$,
            dimension_row.partitioning_func_schema,
            dimension_row.partitioning_func,
            dimension_row.column_name,
            dimension_slice_row.range_start);
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(
            $$
                %1$I.%2$I(%3$I) < %4$L
            $$,
            dimension_row.partitioning_func_schema,
            dimension_row.partitioning_func,
            dimension_row.column_name,
            dimension_slice_row.range_end);
        END IF;

        return array_to_string(parts, 'AND');
    ELSE
        --TODO: only works with time for now
        IF _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimension_row.column_type) =
           _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type) THEN
            RAISE 'Time based constraints have the same start and end values for column "%": %',
                    dimension_row.column_name,
                    _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type);
        END IF;

        parts = ARRAY[]::text[];

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(
            $$
                 %1$I >= %2$s
            $$,
            dimension_row.column_name,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimension_row.column_type));
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(
            $$
                 %1$I < %2$s
            $$,
            dimension_row.column_name,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimension_row.column_type));
        END IF;

        return array_to_string(parts, 'AND');
    END IF;
END
$BODY$;

--has to be done since old range_end for the CHECK constraint was 2147483647 on closed partitions.
DO $$
DECLARE
    chunk_constraint_row  _timescaledb_catalog.chunk_constraint;
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    -- Need to do this update in two loops: first remove constraints, then add back.
    -- This is because we can only remove the old partitioning function when
    -- there are no constraints on the tables referencing the old function
    FOR chunk_constraint_row IN
        SELECT cc.*
        FROM _timescaledb_catalog.chunk_constraint cc
        INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (cc.dimension_slice_id = ds.id)
        INNER JOIN  _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
        WHERE d.partitioning_func IS NOT NULL
    LOOP
        SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;

        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', chunk_row.schema_name, chunk_row.table_name, chunk_constraint_row.constraint_name);
    END LOOP;

    RAISE NOTICE 'Updating constraints';

    DROP FUNCTION IF EXISTS _timescaledb_internal.get_partition_for_key(text);
    CREATE OR REPLACE FUNCTION _timescaledb_internal.get_partition_for_key(val anyelement)
    RETURNS int
    AS '@MODULE_PATHNAME@', 'ts_get_partition_for_key' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

    FOR chunk_constraint_row IN
        SELECT cc.*
        FROM _timescaledb_catalog.chunk_constraint cc
        INNER JOIN  _timescaledb_catalog.dimension_slice ds ON (cc.dimension_slice_id = ds.id)
        INNER JOIN  _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
        WHERE d.partitioning_func IS NOT NULL
    LOOP
        SELECT * INTO STRICT chunk_row FROM _timescaledb_catalog.chunk c WHERE c.id = chunk_constraint_row.chunk_id;
        PERFORM _timescaledb_internal.chunk_constraint_add_table_constraint(chunk_constraint_row);
    END LOOP;

END$$;

--for timestamp (non-tz) columns we used to have internal_time -> constraint_time via local_time.
--So the internal time was interpreted as UTC but the constraint was printed in terms of the local time.
--Now we interpret the internal_time as UTC and the constraints is generated as UTC as well.
--These constraints should not be re-written since they are correct for the data. But we should adjust the internal time
--to be consistent.

-- So _timescaledb_internal.to_timestamp(internal_time)::timestamp gives you the old constraint
-- We then convert it to timestamptz as though it was at UTC
-- finally, we convert it to the internal represtentation back.

UPDATE _timescaledb_catalog.dimension_slice ds
SET
range_end = _timescaledb_internal.to_unix_microseconds(timezone('UTC',_timescaledb_internal.to_timestamp(range_end)::timestamp)),
range_start = _timescaledb_internal.to_unix_microseconds(timezone('UTC',_timescaledb_internal.to_timestamp(range_start)::timestamp))
FROM _timescaledb_catalog.dimension d
WHERE ds.dimension_id = d.id AND d.column_type = 'timestamp'::regtype;
