--calculate new times for a new chunk for appropriate time values
--Should not be called directly. Requires a lock on chunk table
CREATE OR REPLACE FUNCTION _timescaledb_meta.calculate_new_chunk_times(
        partition_id INT,
        "time"       BIGINT,
    OUT table_start  BIGINT,
    OUT table_end    BIGINT
)
LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    partition_epoch_row _timescaledb_catalog.partition_epoch;
    chunk_row           _timescaledb_catalog.chunk;
    time_interval BIGINT;
BEGIN
    SELECT pe.*
    INTO partition_epoch_row
    FROM _timescaledb_catalog.partition p
    INNER JOIN _timescaledb_catalog.partition_epoch pe ON (p.epoch_id = pe.id)
    WHERE p.id = partition_id
    FOR SHARE;

    SELECT chunk_time_interval
    INTO time_interval
    FROM _timescaledb_catalog.hypertable ht
    WHERE ht.id = partition_epoch_row.hypertable_id;

    -- Create start and stop times for the new chunk, subtract 1 from the end time
    -- as chunk intervals are inclusive.
    table_start := ("time" / time_interval) * time_interval;
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
        --there is at least one table that starts in the new chunk, cut the end to match
        --the first of them
        table_end := chunk_row.start_time - 1;
    END IF;
END
$BODY$;

--creates chunk.
CREATE OR REPLACE FUNCTION _timescaledb_meta.create_chunk_row(
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
    FROM _timescaledb_meta.calculate_new_chunk_times(part_id, time_point);

    INSERT INTO _timescaledb_catalog.chunk (partition_id, start_time, end_time)
    VALUES (part_id, table_start, table_end);
END
$BODY$;

--creates and returns a new chunk, taking a lock on the partition being modified.
CREATE OR REPLACE FUNCTION _timescaledb_meta.create_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row     _timescaledb_catalog.chunk;
    partition_row _timescaledb_catalog.partition;
BEGIN
    PERFORM set_config('timescaledb_internal.originating_node', 'true', true);
    LOCK TABLE _timescaledb_catalog.chunk IN EXCLUSIVE MODE;

    --recheck:
    chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);

    IF chunk_row IS NULL THEN
        PERFORM _timescaledb_meta.create_chunk_row(partition_id, time_point);
        chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);
    END IF;

    IF chunk_row IS NULL THEN --recheck
        RAISE EXCEPTION 'Should never happen: chunk not found after creation on meta'
        USING ERRCODE = 'IO501';
    END IF;

    RETURN chunk_row;
END
$BODY$;


--gets or creates chunk. Concurrent chunk creation is prevented at the
--partition level by taking a lock on the partition being modified.
CREATE OR REPLACE FUNCTION _timescaledb_meta.get_or_create_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS _timescaledb_catalog.chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row           _timescaledb_catalog.chunk;
    chunk_table_name    NAME;
BEGIN
    chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);

    IF chunk_row IS NULL THEN
        chunk_row := _timescaledb_meta.create_chunk(partition_id, time_point);
    END IF;

    RETURN chunk_row;
END
$BODY$;
