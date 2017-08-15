CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get_dimension_constraint_sql(
    dimension_id        INTEGER,
    dimension_value     BIGINT
)
 RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format($$
    SELECT cc.chunk_id
    FROM _timescaledb_catalog.dimension_slice ds
    INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id)
    WHERE ds.dimension_id = %1$L and ds.range_start <= %2$L and ds.range_end > %2$L
    $$,
    dimension_id, dimension_value);
$BODY$;

-- get a chunk if it exists
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get_dimensions_constraint_sql(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$

SELECT string_agg(_timescaledb_internal.chunk_get_dimension_constraint_sql(dimension_id,
                                                                dimension_value),
                                                            ' INTERSECT ')
FROM (SELECT unnest(dimension_ids) AS dimension_id,
             unnest(dimension_values) AS dimension_value
     ) AS sub;
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_id_get_by_dimensions(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS SETOF INTEGER LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    IF array_length(dimension_ids, 1) > 0 THEN
        RETURN QUERY EXECUTE _timescaledb_internal.chunk_get_dimensions_constraint_sql(dimension_ids,
        dimension_values);
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    SELECT *
    INTO chunk_row
    FROM _timescaledb_catalog.chunk
    WHERE
    id = (SELECT _timescaledb_internal.chunk_id_get_by_dimensions(dimension_ids,
                                                          dimension_values));
    RETURN chunk_row;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
END
$BODY$;

--todo: unit test
CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range(
        dimension_id      INTEGER,
        dimension_value   BIGINT,
    OUT range_start  BIGINT,
    OUT range_end    BIGINT)
LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    dimension_row _timescaledb_catalog.dimension;
    inter BIGINT;
BEGIN
    SELECT *
    FROM _timescaledb_catalog.dimension
    INTO STRICT dimension_row
    WHERE id = dimension_id;

    IF dimension_row.interval_length IS NOT NULL THEN
        -- For positive values, integer division finds a lower bound which is BEFORE
        -- the value we want. For negative values, integer divsion finds a upper bound which
        -- is AFTER the value we want. Therefore for positive numbers we find the
        -- range_start via integer division, while for negative we find the range_end.
        IF dimension_value >= 0 THEN
            range_start := (dimension_value / dimension_row.interval_length) * dimension_row.interval_length;
            range_end := range_start + dimension_row.interval_length;
        ELSE
            range_end := (dimension_value / dimension_row.interval_length) * dimension_row.interval_length;
            range_start := range_end - dimension_row.interval_length;
        END IF;
    ELSE
       inter := (2147483647 / dimension_row.num_slices);
       IF dimension_value >= inter * (dimension_row.num_slices - 1) THEN
          --put overflow from integer-division errors in last range
          range_start = inter * (dimension_row.num_slices - 1);
          range_end = 2147483647;
       ELSE
          range_start = (dimension_value / inter) * inter;
          range_end := range_start + inter;
       END IF;
    END IF;
END
$BODY$;

-- calculate the range for a free dimension.
-- assumes one other fixed dimension.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_calculate_new_ranges(
        free_dimension_id      INTEGER,
        free_dimension_value   BIGINT,
        fixed_dimension_ids    INTEGER[],
        fixed_dimension_values BIGINT[],
        align                  BOOLEAN,
    OUT new_range_start        BIGINT,
    OUT new_range_end          BIGINT
)
LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    overlap_value BIGINT;
    alignment_found BOOLEAN := FALSE;
BEGIN
    new_range_start := NULL;
    new_range_end := NULL;

    IF align THEN
        --if i am aligning then fix see if other chunks have values that fit me in the free dimension
        SELECT free_slice.range_start, free_slice.range_end
        INTO new_range_start, new_range_end
        FROM _timescaledb_catalog.chunk c
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.chunk_id = c.id)
        INNER JOIN _timescaledb_catalog.dimension_slice free_slice ON (free_slice.id = cc.dimension_slice_id AND free_slice.dimension_id = free_dimension_id)
        WHERE
           free_slice.range_end > free_dimension_value and free_slice.range_start <= free_dimension_value
        LIMIT 1;

        SELECT new_range_start IS NOT NULL INTO alignment_found;
    END IF;

    IF NOT alignment_found THEN
        --either not aligned or did not find an alignment
        SELECT *
        INTO new_range_start, new_range_end
        FROM _timescaledb_internal.dimension_calculate_default_range(free_dimension_id, free_dimension_value);
    END IF;

    -- Check whether the new chunk interval overlaps with existing chunks.
    -- new_range_start overlaps
    SELECT free_slice.range_end
    INTO overlap_value
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.chunk_id = c.id)
    INNER JOIN _timescaledb_catalog.dimension_slice free_slice ON (free_slice.id = cc.dimension_slice_id AND free_slice.dimension_id = free_dimension_id)
    WHERE
    c.id = (
               SELECT _timescaledb_internal.chunk_id_get_by_dimensions(free_dimension_id || fixed_dimension_ids,
                                                         new_range_start || fixed_dimension_values)
           )
    ORDER BY free_slice.range_end DESC
    LIMIT 1;

    IF FOUND THEN
        -- There is a chunk that overlaps with new_range_start, cut
        -- new_range_start to begin where that chunk ends
        IF alignment_found THEN
            RAISE EXCEPTION 'Should never happen: needed to cut an aligned dimension.
            Free_dimension %. Existing(end): %, New(start):%',
            free_dimension_id, overlap_value, new_range_start
            USING ERRCODE = 'IO501';
        END IF;
        new_range_start := overlap_value;
    END IF;

    --check for new_range_end overlap
    SELECT free_slice.range_start
    INTO overlap_value
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.chunk_constraint cc
    ON (cc.chunk_id = c.id)
    INNER JOIN _timescaledb_catalog.dimension_slice free_slice
    ON (free_slice.id = cc.dimension_slice_id AND free_slice.dimension_id = free_dimension_id)
    WHERE
    c.id = (
        SELECT _timescaledb_internal.chunk_id_get_by_dimensions(free_dimension_id || fixed_dimension_ids,
                                                         new_range_end - 1 || fixed_dimension_values)
    )
    ORDER BY free_slice.range_start ASC
    LIMIT 1;

    IF FOUND THEN
        -- there is at least one table that starts inside, cut the end to match
        IF alignment_found THEN
            RAISE EXCEPTION 'Should never happen: needed to cut an aligned dimension.
            Free_dimension %. Existing(start): %, New(end):%',
            free_dimension_id, overlap_value, new_range_end
            USING ERRCODE = 'IO501';
        END IF;
        new_range_end := overlap_value;
    END IF;
END
$BODY$;

-- creates the row in the chunk table. Prerequisite: appropriate lock.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_create_after_lock(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    dimension_row _timescaledb_catalog.dimension;
    hypertable_id INTEGER;
    free_index INTEGER;
    fixed_dimension_ids INTEGER[];
    fixed_values BIGINT[];
    free_range_start BIGINT;
    free_range_end BIGINT;
    slice_ids INTEGER[];
    slice_id INTEGER;
BEGIN
    SELECT d.hypertable_id
    INTO STRICT hypertable_id
    FROM _timescaledb_catalog.dimension d
    WHERE d.id = dimension_ids[1];

    slice_ids = NULL;
    FOR free_index IN 1 .. array_upper(dimension_ids, 1)
    LOOP
        --keep one dimension free and the rest fixed
        fixed_dimension_ids = dimension_ids[:free_index-1]
                              || dimension_ids[free_index+1:];
        fixed_values = dimension_values[:free_index-1]
                              || dimension_values[free_index+1:];

        SELECT *
        INTO STRICT dimension_row
        FROM _timescaledb_catalog.dimension
        WHERE id = dimension_ids[free_index];

        SELECT *
        INTO free_range_start, free_range_end
        FROM _timescaledb_internal.chunk_calculate_new_ranges(
            dimension_ids[free_index], dimension_values[free_index],
            fixed_dimension_ids, fixed_values, dimension_row.aligned);

        --do not use RETURNING here (ON CONFLICT DO NOTHING)
        INSERT INTO  _timescaledb_catalog.dimension_slice
        (dimension_id, range_start, range_end)
        VALUES(dimension_ids[free_index], free_range_start, free_range_end)
        ON CONFLICT DO NOTHING;

        SELECT id INTO STRICT slice_id
        FROM _timescaledb_catalog.dimension_slice ds
        WHERE ds.dimension_id = dimension_ids[free_index] AND
        ds.range_start = free_range_start AND ds.range_end = free_range_end;

        slice_ids = slice_ids || slice_id;
    END LOOP;

    WITH chunk AS (
        INSERT INTO _timescaledb_catalog.chunk (id, hypertable_id, schema_name, table_name)
        SELECT seq_id, h.id, h.associated_schema_name,
           format('%s_%s_chunk', h.associated_table_prefix, seq_id)
        FROM
        nextval(pg_get_serial_sequence('_timescaledb_catalog.chunk','id')) seq_id,
        _timescaledb_catalog.hypertable h
        WHERE h.id = hypertable_id
        RETURNING *
    )
    INSERT INTO _timescaledb_catalog.chunk_constraint (dimension_slice_id, chunk_id)
    SELECT slice_id_to_insert, chunk.id FROM chunk, unnest(slice_ids) AS slice_id_to_insert;
END
$BODY$;

-- Creates and returns a new chunk, taking a lock on the chunk table.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_create(
    dimension_ids           INTEGER[],
    dimension_values        BIGINT[]
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE
    SECURITY DEFINER SET search_path = ''
    AS
$BODY$
DECLARE
    chunk_row     _timescaledb_catalog.chunk;
BEGIN
    LOCK TABLE _timescaledb_catalog.chunk IN EXCLUSIVE MODE;

    -- recheck:
    chunk_row := _timescaledb_internal.chunk_get(dimension_ids, dimension_values);

    IF chunk_row IS NULL THEN
        PERFORM _timescaledb_internal.chunk_create_after_lock(dimension_ids, dimension_values);
        chunk_row := _timescaledb_internal.chunk_get(dimension_ids, dimension_values);
    END IF;

    IF chunk_row IS NULL THEN -- recheck
        RAISE EXCEPTION 'Should never happen: chunk not found after creation'
        USING ERRCODE = 'IO501';
    END IF;

    RETURN chunk_row;
END
$BODY$;

-- Trigger for when chunk rows are changed.
-- On Insert: create chunk table, add indexes, add triggers.
-- On Delete: drop table
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    kind                  pg_class.relkind%type;
    hypertable_row        _timescaledb_catalog.hypertable;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM _timescaledb_internal.chunk_create_table(NEW.id);

        PERFORM _timescaledb_internal.create_chunk_index_row(NEW.schema_name, NEW.table_name,
                                hi.main_schema_name, hi.main_index_name, hi.definition)
        FROM _timescaledb_catalog.hypertable_index hi
        WHERE hi.hypertable_id = NEW.hypertable_id;

        SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable WHERE id = NEW.hypertable_id;

        PERFORM _timescaledb_internal.create_chunk_trigger(NEW.id, tgname,
            _timescaledb_internal.get_general_trigger_definition(oid))
        FROM pg_trigger
        WHERE tgrelid = format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass
        AND _timescaledb_internal.need_chunk_trigger(NEW.hypertable_id, oid);

        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        -- when deleting the chunk row from the metadata table,
        -- also DROP the actual chunk table that holds data.
        -- Note that the table could already be deleted in case this
        -- trigger fires as a result of a DROP TABLE on the hypertable
        -- that this chunk belongs to.

        EXECUTE format(
                $$
                SELECT c.relkind FROM pg_class c WHERE relname = '%I' AND relnamespace = '%I'::regnamespace
                $$, OLD.table_name, OLD.schema_name
        ) INTO kind;

        IF kind IS NULL THEN
            RETURN OLD;
        END IF;

        EXECUTE format(
            $$
            DROP TABLE %I.%I
            $$, OLD.schema_name, OLD.table_name
        );
        RETURN OLD;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
