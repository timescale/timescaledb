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
CREATE OR REPLACE FUNCTION _meta.create_chunk(
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

--gets or creates chunk. If creating chunk takes a lock on the corresponding partition.
--This prevents concurrently creating chunks on same partitions. 
CREATE OR REPLACE FUNCTION _meta.get_or_create_chunk(
    partition_id INT,
    time_point   BIGINT
)
    RETURNS chunk LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_row     chunk;
    partition_row partition;
BEGIN
    chunk_row := _sysinternal.get_chunk(partition_id, time_point);

    --uses double-checked locking
    IF chunk_row IS NULL THEN
        --get lock
        SELECT *
        INTO partition_row
        FROM partition
        WHERE id = partition_id
        FOR UPDATE;
        --recheck:
        chunk_row := _sysinternal.get_chunk(partition_id, time_point);

        IF chunk_row IS NULL THEN
            PERFORM _meta.create_chunk(partition_id, time_point);
        END IF;

        chunk_row := _sysinternal.get_chunk(partition_id, time_point);

        IF chunk_row IS NULL THEN --recheck
            RAISE EXCEPTION 'Should never happen: chunk not found after creation on meta'
            USING ERRCODE = 'IO501';
        END IF;
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
    table_end        BIGINT;
    chunk_row        chunk;
BEGIN
    SELECT *
    INTO chunk_row
    FROM chunk c
    WHERE c.id = chunk_id
    FOR UPDATE;

    --PHASE 1: lock chunk row on all rows (prevents concurrent chunk insert)
    FOR node_row IN
    SELECT *
    FROM public.node n
    LOOP
        PERFORM dblink_connect(node_row.server_name, node_row.server_name);
        PERFORM dblink_exec(node_row.server_name, 'BEGIN');
        PERFORM 1
        FROM dblink(node_row.server_name, format('SELECT * FROM _sysinternal.lock_for_chunk_close(%L)',
                                                 chunk_id)) AS t(x TEXT);
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
        SELECT t.max_time
        INTO max_time_replica
        FROM dblink(crn_node_row.server_name,
                    format('SELECT * FROM _sysinternal.max_time_for_chunk_close(%L, %L)', crn_node_row.schema_name,
                           crn_node_row.table_name)) AS t(max_time BIGINT);

        IF max_time = 0 THEN
            max_time := max_time_replica;
        ELSIF max_time <> max_time_replica THEN
            RAISE EXCEPTION 'Should never happen: max_time % not equal max_time_replica %', max_time, max_time_replica
            USING ERRCODE = 'IO501';
        END IF;
    END LOOP;

    --TODO: is this right?
    table_end :=((coalesce(max_time, chunk_row.start_time, 0) :: BIGINT / (1e9 * 60 * 60 * 24) + 1) :: BIGINT) *
                (1e9 * 60 * 60 * 24) :: BIGINT - 1;

    --set time locally
    UPDATE chunk
    SET end_time = table_end
    WHERE id = chunk_id;

    --PHASE 3: set max_time remotely
    FOR node_row IN
    SELECT *
    FROM node n
    LOOP
        PERFORM 1
        FROM dblink(node_row.server_name,
                    format('SELECT * FROM _sysinternal.set_end_time_for_chunk_close(%L, %L)', chunk_id, table_end)) AS t(x TEXT);
        PERFORM dblink_exec(node_row.server_name, 'COMMIT');
        --TODO: should we disconnect here?
        PERFORM dblink_disconnect(node_row.server_name);
    END LOOP;
END
$BODY$;
