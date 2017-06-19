-- get the chunk for a given partition and time.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE SQL STABLE AS
$BODY$
SELECT *
FROM _timescaledb_catalog.chunk c
WHERE c.partition_id = get_chunk.partition_id AND
      (c.start_time <= time_point OR c.start_time IS NULL) AND
      (c.end_time >= time_point OR c.end_time IS NULL);
$BODY$;

-- calculate new times for a new chunk for appropriate time values
-- Should not be called directly. Requires a lock on chunk table
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.calculate_new_chunk_times(
        partition_id INT,
        time_point   BIGINT,
    OUT table_start  BIGINT,
    OUT table_end    BIGINT
)
LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    partition_epoch_row _timescaledb_catalog.partition_epoch;
    chunk_row           _timescaledb_catalog.chunk;
    time_interval       BIGINT;
    start_distinct_ctn  BIGINT;
    end_distinct_ctn    BIGINT;
BEGIN

    SELECT pe.*
    INTO partition_epoch_row
    FROM _timescaledb_catalog.partition p
    INNER JOIN _timescaledb_catalog.partition_epoch pe ON (p.epoch_id = pe.id)
    WHERE p.id = partition_id
    FOR SHARE;

    -- Check if there are chunks in other partitions.
    -- If so, use their time range to keep chunks time aligned

    SELECT COUNT(DISTINCT(start_time)), COUNT(DISTINCT(end_time))
    INTO start_distinct_ctn, end_distinct_ctn
    FROM _timescaledb_catalog.chunk c
    INNER JOIN _timescaledb_catalog.partition p ON (p.id = c.partition_id)
    WHERE c.start_time <= time_point AND
          c.end_time >= time_point AND
          p.epoch_id = partition_epoch_row.id;

    -- Sanity check that all previous chunks between partitions are
    -- aligned
    IF start_distinct_ctn != 0 or end_distinct_ctn != 0 THEN
        -- At least one chunk exists, check that all chunks 
        -- from other partitions are aligned
        IF start_distinct_ctn != 1 OR end_distinct_ctn != 1 THEN
           RAISE EXCEPTION 'Unaligned chunks between partitions';
        END IF;

        SELECT start_time, end_time 
        INTO table_start, table_end
        FROM _timescaledb_catalog.chunk AS c
        INNER JOIN _timescaledb_catalog.partition p ON (p.id = c.partition_id)
        WHERE c.start_time <= time_point AND
              c.end_time >= time_point AND
              p.epoch_id = partition_epoch_row.id LIMIT 1;    

        RETURN; 
    END IF;

    SELECT chunk_time_interval
    INTO time_interval
    FROM _timescaledb_catalog.hypertable ht
    WHERE ht.id = partition_epoch_row.hypertable_id;

    -- Create start and stop times for the new chunk, subtract 1 from the end time
    -- as chunk intervals are inclusive.
    table_start := (time_point / time_interval) * time_interval;
    table_end := table_start + time_interval - 1;

    -- Check whether the new chunk interval overlaps with existing chunks.
    SELECT *
    INTO chunk_row
    FROM _timescaledb_catalog.chunk AS c
    WHERE c.end_time <= table_end AND
          c.end_time >= table_start AND
          c.partition_id = calculate_new_chunk_times.partition_id
    ORDER BY c.end_time DESC
    LIMIT 1;

    IF FOUND THEN
        -- there is at least one table that ends in the new chunk, cut the
        -- start to match the last one
        table_start := chunk_row.end_time + 1;
    END IF;

    SELECT *
    INTO chunk_row
    FROM _timescaledb_catalog.chunk AS c
    WHERE c.start_time < table_end AND
          c.start_time >= table_start AND
          c.partition_id = calculate_new_chunk_times.partition_id
    ORDER BY c.start_time ASC
    LIMIT 1;

    IF FOUND THEN
        -- there is at least one table that starts in the new chunk, cut the end to match
        -- the first of them
        table_end := chunk_row.start_time - 1;
    END IF;
END
$BODY$;

-- creates the row in the chunk table. Prerequisite: appropriate lock and check that chunk does not
-- already exist.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk_row(
    part_id     INT,
    time_point  BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    table_start BIGINT;
    table_end   BIGINT;
BEGIN    
    
    SELECT *
    INTO table_start, table_end
    FROM _timescaledb_internal.calculate_new_chunk_times(part_id, time_point);

    INSERT INTO _timescaledb_catalog.chunk (id, partition_id, start_time, end_time, schema_name, table_name)
    SELECT seq_id, part_id, table_start, table_end, h.associated_schema_name, 
           format('%s_%s_chunk', h.associated_table_prefix, seq_id)
    FROM 
    nextval(pg_get_serial_sequence('_timescaledb_catalog.chunk','id')) seq_id,
    _timescaledb_catalog.partition p
    INNER JOIN _timescaledb_catalog.partition_epoch pe ON (pe.id = p.epoch_id)
    INNER JOIN _timescaledb_catalog.hypertable h ON (h.id = pe.hypertable_id)
    WHERE p.id = part_id;
END
$BODY$;

-- Creates and returns a new chunk, taking a lock on the chunk table.
-- static
CREATE OR REPLACE FUNCTION _timescaledb_internal.create_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row     _timescaledb_catalog.chunk;
    partition_row _timescaledb_catalog.partition;
BEGIN
    LOCK TABLE _timescaledb_catalog.chunk IN EXCLUSIVE MODE;

    -- recheck:
    chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);

    IF chunk_row IS NULL THEN
        PERFORM _timescaledb_internal.create_chunk_row(partition_id, time_point);
        chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);
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
CREATE OR REPLACE FUNCTION _timescaledb_internal.get_or_create_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row _timescaledb_catalog.chunk;
BEGIN
    chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);

    IF chunk_row IS NULL THEN
        chunk_row := _timescaledb_internal.create_chunk(partition_id, time_point);
    END IF;

    RETURN chunk_row;
END
$BODY$;
