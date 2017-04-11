--calculate new times for a new chunks for appropriate time values
--tables are created open-ended in one or two direction (either start_time or end_time is NULL)
--that way, tables grow in some time dir.
--Tables are always created adjacent to existing tables. So, tables
--will never be disjoint in terms of time. Therefore you will have:
--     <---current open ended start_time table --| existing closed tables | -- current open ended end_time table --->
--Should not be called directly. Requires a lock on partition (prevents simultaneous inserts)
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
BEGIN
    SELECT pe.*
    INTO partition_epoch_row
    FROM _timescaledb_catalog.partition p
    INNER JOIN _timescaledb_catalog.partition_epoch pe ON (p.epoch_id = pe.id)
    WHERE p.id = partition_id
    FOR SHARE;

    table_start := partition_epoch_row.start_time;
    table_end := partition_epoch_row.end_time;

    SELECT *
    INTO chunk_row
    FROM _timescaledb_catalog.chunk AS c
    WHERE c.end_time < calculate_new_chunk_times."time" AND
          c.partition_id = calculate_new_chunk_times.partition_id
    ORDER BY c.end_time DESC
    LIMIT 1
    FOR SHARE;

    IF FOUND THEN
        --there is a table that ends before this point;
        table_start := chunk_row.end_time + 1;
    ELSE
        SELECT *
        INTO chunk_row
        FROM _timescaledb_catalog.chunk AS c
        WHERE c.start_time > calculate_new_chunk_times."time" AND
              c.partition_id = calculate_new_chunk_times.partition_id
        ORDER BY c.start_time DESC
        LIMIT 1
        FOR SHARE;
        IF FOUND THEN
            --there is a table that ends before this point
            table_end := chunk_row.start_time - 1;
        END IF;
    END IF;
END
$BODY$;

--creates chunk.
CREATE OR REPLACE FUNCTION _timescaledb_meta.create_chunk_unlocked(
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

    --INSERT on chunk implies SHARE lock on partition row due to foreign key.
    --If the insert conflicts, it means another transaction created the chunk
    --before us, and we can safely ignore the error.
    INSERT INTO _timescaledb_catalog.chunk (partition_id, start_time, end_time)
    VALUES (part_id, table_start, table_end)
    ON CONFLICT (partition_id, start_time) DO NOTHING;
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
     --get lock: prevents simultaneous creation of multiple chunks for same partition.
     SELECT *
     INTO partition_row
     FROM  _timescaledb_catalog.partition
     WHERE id = partition_id
     FOR UPDATE; 

    --recheck:
    chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);

    IF chunk_row IS NULL THEN
        PERFORM _timescaledb_meta.create_chunk_unlocked(partition_id, time_point);
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
    chunk_max_size      BIGINT;
BEGIN
    chunk_row := _timescaledb_internal.get_chunk(partition_id, time_point);

    IF chunk_row IS NULL THEN
        chunk_row := _timescaledb_meta.create_chunk(partition_id, time_point);
    END IF;

    RETURN chunk_row;
END
$BODY$;

CREATE OR REPLACE FUNCTION _timescaledb_meta.close_chunk_end(
    chunk_id INT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    crn_node_row     RECORD;
    node_row         _timescaledb_catalog.node;
    max_time_replica BIGINT;
    max_time         BIGINT = 0;
    chunk_row        _timescaledb_catalog.chunk;
    partition_row    _timescaledb_catalog.partition;
BEGIN
    --get chunk lock
    SELECT *
    INTO chunk_row
    FROM _timescaledb_catalog.chunk c
    WHERE c.id = chunk_id AND end_time IS NULL
    FOR UPDATE;

    IF chunk_row IS NULL THEN
        --should only happen if the chunk was already closed
        SELECT *
        INTO chunk_row
        FROM _timescaledb_catalog.chunk c
        WHERE c.id = chunk_id AND end_time IS NOT NULL;

        IF chunk_row IS NULL THEN
            RAISE EXCEPTION 'Should never happen: chunk not found in close.'
            USING ERRCODE = 'IO501';
        END IF;

        RETURN;
    END IF;

    --PHASE 1: lock chunk row on all nodes (prevents concurrent chunk insert)
    FOR node_row IN
    SELECT *
    FROM _timescaledb_catalog.node n
    LOOP
        PERFORM _timescaledb_data_api.lock_for_chunk_close(node_row.database_name, chunk_id);
    END LOOP;

    --PHASE 2: get max time for chunk
    FOR crn_node_row IN
    SELECT
        crn.*,
        n.*
    FROM _timescaledb_catalog.chunk_replica_node crn
    INNER JOIN _timescaledb_catalog.node n ON (n.database_name = crn.database_name)
    WHERE crn.chunk_id = close_chunk_end.chunk_id
    LOOP
        SELECT *
        INTO max_time_replica
        FROM  _timescaledb_data_api.max_time_for_chunk_close(
            crn_node_row.database_name,
            crn_node_row.schema_name,
            crn_node_row.table_name
        );


        IF max_time = 0 THEN
            max_time := max_time_replica;
        ELSIF max_time <> max_time_replica THEN
            RAISE EXCEPTION 'Should never happen: max_time % not equal max_time_replica %', max_time, max_time_replica
            USING ERRCODE = 'IO501';
        END IF;
    END LOOP;

    IF max_time IS NULL THEN
        -- No rows in chunk, so closing is a NOP.
        RAISE WARNING 'Cannot close an empty chunk table';
        RETURN;
    END IF;

    --set time locally
    UPDATE _timescaledb_catalog.chunk
    SET end_time = max_time
    WHERE id = chunk_id;

    --PHASE 3: set max_time remotely
    FOR node_row IN
    SELECT *
    FROM _timescaledb_catalog.node n
    WHERE n.database_name <> current_database()
    LOOP
        PERFORM _timescaledb_data_api.set_end_time_for_chunk_close(
            node_row.database_name,
            chunk_id,
            max_time
        );
    END LOOP;
END
$BODY$;
