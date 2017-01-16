--calculate new times for a new chunks for appropriate time values
--tables are created open-ended in one or two direction (either start_time or end_time is NULL)
--that way, tables grow in some time dir.
--Tables are always created adjacent to existing tables. So, tables
--will never be disjoint in terms of time. Therefore you will have:
--     <---current open ended start_time table --| existing closed tables | -- current open ended end_time table --->
--Should not be called directly. Requires a lock on partition (prevents simultaneous inserts)
CREATE OR REPLACE FUNCTION _meta.calculate_new_chunk_times(
        partition_id INT,
        "time"       BIGINT,
    OUT table_start  BIGINT,
    OUT table_end    BIGINT
)
LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    partition_epoch_row partition_epoch;
    chunk_row           chunk;
BEGIN
    SELECT pe.*
    INTO partition_epoch_row
    FROM partition p
    INNER JOIN partition_epoch pe ON (p.epoch_id = pe.id)
    WHERE p.id = partition_id
    FOR SHARE;

    table_start := partition_epoch_row.start_time;
    table_end := partition_epoch_row.end_time;

    SELECT *
    INTO chunk_row
    FROM chunk AS c
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
        FROM chunk AS c
        WHERE c.start_time > calculate_new_chunk_times."time" AND
              c.partition_id = calculate_new_chunk_times.partition_id
        ORDER BY c.start_time DESC
        LIMIT 1
        FOR SHARE;
        IF FOUND THEN
            --there is a table that ends before this point
            table_end = chunk_row.start_time - 1;
        END IF;
    END IF;
END
$BODY$;

--creates chunk. Must be called after aquiring a lock on partition.
CREATE OR REPLACE FUNCTION _meta.create_chunk_unlocked(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    table_start BIGINT;
    table_end   BIGINT;
BEGIN
    SELECT *
    INTO table_start, table_end
    FROM _meta.calculate_new_chunk_times(partition_id, time_point);

    INSERT INTO chunk (partition_id, start_time, end_time)
    VALUES (partition_id, table_start, table_end);
END
$BODY$;

--creates and returns a new chunk, taking a lock on the partition being modified.
CREATE OR REPLACE FUNCTION _meta.create_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row     chunk;
    partition_row partition;
BEGIN
    --get lock
    SELECT *
    INTO partition_row
    FROM partition
    WHERE id = partition_id
    FOR UPDATE;

    --recheck:
    chunk_row := _sysinternal.get_chunk(partition_id, time_point);

    IF chunk_row IS NULL THEN
        PERFORM _meta.create_chunk_unlocked(partition_id, time_point);
        chunk_row := _sysinternal.get_chunk(partition_id, time_point);
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
CREATE OR REPLACE FUNCTION _meta.get_or_create_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row           chunk;
    chunk_table_name    NAME;
    chunk_max_size      BIGINT;
BEGIN
    chunk_row := _sysinternal.get_chunk(partition_id, time_point);

    IF chunk_row IS NULL THEN
        chunk_row := _meta.create_chunk(partition_id, time_point);
    END IF;

    RETURN chunk_row;
END
$BODY$;

CREATE OR REPLACE FUNCTION _meta.close_chunk_end(
    chunk_id INT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    crn_node_row     RECORD;
    node_row         node;
    max_time_replica BIGINT;
    max_time         BIGINT = 0;
    chunk_row        chunk;
    partition_row    partition;
BEGIN
    --get chunk lock
    SELECT *
    INTO chunk_row
    FROM chunk c
    WHERE c.id = chunk_id
    FOR UPDATE;

    --get partition lock
    SELECT *
    INTO partition_row
    FROM partition
    WHERE id = chunk_row.partition_id
    FOR UPDATE;

    -- the chunk is already closed if its end time is set
    IF chunk_row.end_time IS NOT NULL THEN
        RETURN;
    END IF;

    --PHASE 1: lock chunk row on all rows (prevents concurrent chunk insert)
    FOR node_row IN
    SELECT *
    FROM public.node n
    LOOP
        PERFORM _meta.node_transaction_exec_with_return(node_row.database_name, node_row.server_name,
            format('SELECT * FROM _sysinternal.lock_for_chunk_close(%L)', chunk_id)
        );
    END LOOP;

    --PHASE 2: get max time for chunk
    FOR crn_node_row IN
    SELECT
        crn.*,
        n.*
    FROM chunk_replica_node crn
    INNER JOIN node n ON (n.database_name = crn.database_name)
    WHERE crn.chunk_id = close_chunk_end.chunk_id
    LOOP
        SELECT t.max_time_return::BIGINT
        INTO max_time_replica
        FROM  _meta.node_transaction_exec_with_return(crn_node_row.database_name, crn_node_row.server_name,
                    format('SELECT * FROM _sysinternal.max_time_for_chunk_close(%L, %L)', 
                        crn_node_row.schema_name,
                        crn_node_row.table_name)
                ) AS t(max_time_return);

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
    UPDATE chunk
    SET end_time = max_time
    WHERE id = chunk_id;

    --PHASE 3: set max_time remotely
    FOR node_row IN
    SELECT *
    FROM node n
    WHERE n.database_name <> current_database()
    LOOP
        PERFORM _meta.node_transaction_exec_with_return(node_row.database_name, node_row.server_name,
                    format('SELECT * FROM _sysinternal.set_end_time_for_chunk_close(%L, %L)', chunk_id, max_time)
                );
    END LOOP;
END
$BODY$;
