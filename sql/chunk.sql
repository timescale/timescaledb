-- get a chunk if it exists
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get_2_dim(
    time_dimension_id           INTEGER,
    time_value                  BIGINT,
    space_dimension_id          INTEGER,
    space_dimension_hash        BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE SQL STABLE AS
$BODY$
SELECT * 
FROM _timescaledb_catalog.chunk
WHERE 
id = (
SELECT cc.chunk_id
FROM _timescaledb_catalog.dimension_slice ds 
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id) 
WHERE ds.dimension_id = time_dimension_id and ds.range_start <= time_value and ds.range_end > time_value

INTERSECT

SELECT cc.chunk_id
FROM _timescaledb_catalog.dimension_slice ds 
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id) 
WHERE ds.dimension_id =  space_dimension_id and ds.range_start <= space_dimension_hash and ds.range_end > space_dimension_hash
)
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get_1_dim(
    time_dimension_id           INTEGER,
    time_value                  BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE SQL STABLE AS
$BODY$
SELECT * 
FROM _timescaledb_catalog.chunk
WHERE 
id = (
SELECT cc.chunk_id
FROM _timescaledb_catalog.dimension_slice ds 
INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id) 
WHERE ds.dimension_id = time_dimension_id and ds.range_start <= time_value and ds.range_end > time_value
)
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get(
    time_dimension_id           INTEGER,
    time_value                  BIGINT,
    space_dimension_id          INTEGER,
    space_value        BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE SQL STABLE AS
$BODY$
SELECT 
CASE WHEN space_dimension_id IS NOT NULL AND space_dimension_id <> 0 THEN
    _timescaledb_internal.chunk_get_2_dim(time_dimension_id, time_value, space_dimension_id, space_value)
ELSE 
    _timescaledb_internal.chunk_get_1_dim(time_dimension_id, time_value)
END;
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
        range_start := (dimension_value / dimension_row.interval_length) * dimension_row.interval_length;
        range_end := range_start + dimension_row.interval_length;
    ELSE
       inter := (65535 / dimension_row.num_slices);
       IF dimension_value >= inter * (dimension_row.num_slices - 1) THEN
          --put overflow from integer-division errors in last range
          range_start = inter * (dimension_row.num_slices - 1);
          range_end = 65535;
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
        fixed_dimension_id     INTEGER,
        fixed_dimension_value  BIGINT,
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
        SELECT cc.chunk_id
        FROM _timescaledb_catalog.dimension_slice ds 
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id) 
        WHERE ds.dimension_id = free_dimension_id and ds.range_end <= new_range_end and ds.range_end > new_range_start

        INTERSECT

        SELECT cc.chunk_id
        FROM _timescaledb_catalog.dimension_slice ds 
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id) 
        WHERE ds.dimension_id =  fixed_dimension_id and ds.range_start <= fixed_dimension_value and ds.range_end > fixed_dimension_value
    )
    ORDER BY free_slice.range_end DESC 
    LIMIT 1;

    IF FOUND THEN
        -- there is at least one ends inside cut the
        -- start to match the last one
        IF alignment_found THEN
            RAISE EXCEPTION 'Should never happen: needed to cut an aligned dimension'
            USING ERRCODE = 'IO501';
        END IF;
        new_range_start := overlap_value;
    END IF;

    --check for new_range_end overlap
    SELECT free_slice.range_start
    INTO overlap_value
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (cc.chunk_id = c.id) 
    INNER JOIN _timescaledb_catalog.dimension_slice free_slice ON (free_slice.id = cc.dimension_slice_id AND free_slice.dimension_id = free_dimension_id)
    WHERE 
    c.id = (
        SELECT cc.chunk_id
        FROM _timescaledb_catalog.dimension_slice ds 
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id) 
        WHERE ds.dimension_id = free_dimension_id and  ds.range_start >= new_range_start and ds.range_start < new_range_end

        INTERSECT

        SELECT cc.chunk_id
        FROM _timescaledb_catalog.dimension_slice ds 
        INNER JOIN _timescaledb_catalog.chunk_constraint cc ON (ds.id = cc.dimension_slice_id) 
        WHERE ds.dimension_id = fixed_dimension_id and ds.range_start <= fixed_dimension_value and ds.range_end > fixed_dimension_value
    )
    ORDER BY free_slice.range_start ASC
    LIMIT 1;

    IF FOUND THEN
        -- there is at least one table that starts inside, cut the end to match
        IF alignment_found THEN
            RAISE EXCEPTION 'Should never happen: needed to cut an aligned dimension'
            USING ERRCODE = 'IO501';
        END IF;
        new_range_end := overlap_value;
    END IF;
END
$BODY$;




-- creates the row in the chunk table. Prerequisite: appropriate lock.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_create_after_lock(
    time_dimension_id           INTEGER,
    time_value                  BIGINT,
    space_dimension_id          INTEGER,
    space_value                 BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    time_start BIGINT;
    time_end   BIGINT;
    space_start BIGINT;
    space_end BIGINT;
    dimension_row _timescaledb_catalog.dimension;
BEGIN
    SELECT *
    INTO STRICT dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE id = time_dimension_id;

    SELECT *
    INTO time_start, time_end
    FROM _timescaledb_internal.chunk_calculate_new_ranges(time_dimension_id, time_value, space_dimension_id, space_value, true);

    --do not use RETURNING here (ON CONFLICT DO NOTHING)
    INSERT INTO  _timescaledb_catalog.dimension_slice (dimension_id, range_start, range_end) 
    VALUES(time_dimension_id, time_start, time_end) ON CONFLICT DO NOTHING;


    IF space_dimension_id IS NOT NULL AND space_dimension_id > 0 THEN
        SELECT *
        INTO space_start, space_end
        FROM _timescaledb_internal.chunk_calculate_new_ranges(space_dimension_id, space_value, time_dimension_id, time_value, false);

        INSERT INTO  _timescaledb_catalog.dimension_slice (dimension_id, range_start, range_end) 
        VALUES(space_dimension_id, space_start, space_end) ON CONFLICT DO NOTHING;
    END IF;

    WITH chunk AS (
        INSERT INTO _timescaledb_catalog.chunk (id, hypertable_id, schema_name, table_name)
        SELECT seq_id, h.id, h.associated_schema_name,
           format('%s_%s_chunk', h.associated_table_prefix, seq_id)
        FROM 
        nextval(pg_get_serial_sequence('_timescaledb_catalog.chunk','id')) seq_id,
        _timescaledb_catalog.hypertable h 
        WHERE h.id = dimension_row.hypertable_id
        RETURNING *
    ), space_slice AS (
        SELECT * 
        FROM _timescaledb_catalog.dimension_slice
        WHERE dimension_id = space_dimension_id AND
              range_start = space_start AND
              range_end = space_end
    ), time_slice AS (
        SELECT * 
        FROM _timescaledb_catalog.dimension_slice
        WHERE dimension_id = time_dimension_id AND
              range_start = time_start AND
              range_end = time_end
    )
    INSERT INTO _timescaledb_catalog.chunk_constraint (dimension_slice_id, chunk_id)
    SELECT space_slice.id, chunk.id FROM space_slice, chunk
    UNION 
    SELECT time_slice.id, chunk.id FROM time_slice, chunk;
END
$BODY$;

-- Creates and returns a new chunk, taking a lock on the chunk table.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_create(
    time_dimension_id           INTEGER,
    time_value                  BIGINT,
    space_dimension_id          INTEGER,
    space_value                 BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row     _timescaledb_catalog.chunk;
BEGIN
    LOCK TABLE _timescaledb_catalog.chunk IN EXCLUSIVE MODE;

    -- recheck:
    chunk_row := _timescaledb_internal.chunk_get(time_dimension_id, time_value, space_dimension_id, space_value);

    IF chunk_row IS NULL THEN
        PERFORM _timescaledb_internal.chunk_create_after_lock(time_dimension_id, time_value, space_dimension_id, space_value);
        chunk_row := _timescaledb_internal.chunk_get(time_dimension_id, time_value, space_dimension_id, space_value);
    END IF;

    IF chunk_row IS NULL THEN -- recheck
        RAISE EXCEPTION 'Should never happen: chunk not found after creation'
        USING ERRCODE = 'IO501';
    END IF;

    RETURN chunk_row;
END
$BODY$;

-- Return a chunk, creating one if it doesn't exist. 
-- This is the only non-static function in this file.
CREATE OR REPLACE FUNCTION _timescaledb_internal.chunk_get_or_create(
    time_dimension_id           INTEGER,
    time_value                  BIGINT,
    space_dimension_id          INTEGER,
    space_value                 BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    chunk_row := _timescaledb_internal.chunk_get(time_dimension_id, time_value, space_dimension_id, space_value);

    IF chunk_row IS NULL THEN
        chunk_row := _timescaledb_internal.chunk_create(time_dimension_id, time_value, space_dimension_id, space_value);
    END IF;

    RETURN chunk_row;
END
$BODY$;
