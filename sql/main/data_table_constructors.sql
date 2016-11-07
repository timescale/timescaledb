--calculate new times for a new data_table for appropriate time values
--tables are created open-ended in one direction (either start_time or end_time is NULL)
--that way, tables grow in some time dir.
--Tables are always created adjacent to existing tables. So, tables
--will never be disjoint in terms of time. Therefore you will have:
--     <---current open ended start_time table --| existing closed tables | -- current open ended end_time table --->
-- tables start and end times are aligned at day boundaries.
--Should not be called directly. Requires a lock on partition (prevents simultaneous inserts)
CREATE OR REPLACE FUNCTION _sysinternal.calculate_new_data_table_times(
        "time"             BIGINT,
        namespace_name     NAME,
        partition_number   SMALLINT,
        total_partitions   SMALLINT,
        partitioning_field NAME,
    OUT table_start        BIGINT,
    OUT table_end          BIGINT
)
LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    data_table_row data_table;
BEGIN
    SELECT *
    INTO data_table_row
    FROM data_table AS dt
    WHERE dt.end_time < calculate_new_data_table_times."time" AND
          dt.namespace_name = calculate_new_data_table_times.namespace_name AND
          dt.partition_number = calculate_new_data_table_times.partition_number AND
          dt.total_partitions = calculate_new_data_table_times.total_partitions AND
          dt.partitioning_field = calculate_new_data_table_times.partitioning_field
    ORDER BY end_time DESC
    LIMIT 1
    FOR SHARE;

    IF NOT FOUND THEN
        SELECT *
        INTO data_table_row
        FROM data_table AS dt
        WHERE dt.start_time > calculate_new_data_table_times."time" AND
              dt.namespace_name = calculate_new_data_table_times.namespace_name AND
              dt.partition_number = calculate_new_data_table_times.partition_number AND
              dt.total_partitions = calculate_new_data_table_times.total_partitions AND
              dt.partitioning_field = calculate_new_data_table_times.partitioning_field
        ORDER BY start_time ASC
        LIMIT 1
        FOR SHARE;

        IF NOT FOUND THEN
            --completely new table
            table_start := (time :: BIGINT / (1e9 * 60 * 60 * 24) :: BIGINT) *
                           (1e9 * 60 * 60 * 24) :: BIGINT; --no tables exist, set to closest earliest date
            table_end := NULL;
        ELSE
            --there is a table already existing that start after this point;
            table_start := NULL;
            table_end := data_table_row.start_time - 1;
        END IF;
    ELSE
        --there is a table that ends before this point;
        table_start := data_table_row.end_time + 1;
        table_end := NULL;
    END IF;
END
$BODY$;

--creates data table. Must be called after aquiring a lock on partition.
CREATE OR REPLACE FUNCTION _sysinternal.create_data_table(
    "time"              BIGINT,
    partition_table_row partition_table
)
    RETURNS data_table LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    table_start            BIGINT;
    table_end              BIGINT;
    table_name             NAME;
    primary_key_index_name NAME;
    table_oid              REGCLASS;
    data_table_row         data_table;
    schema_name            NAME;
BEGIN

    SELECT *
    INTO table_start, table_end
    FROM _sysinternal.calculate_new_data_table_times("time", partition_table_row.namespace_name,
                                                             partition_table_row.partition_number, partition_table_row.total_partitions,
                                                             partition_table_row.partitioning_field);

    table_name := get_data_table_name(partition_table_row.namespace_name, partition_table_row.partition_number,
                                      partition_table_row.total_partitions, partition_table_row.partitioning_field,
                                      table_start, table_end);
    primary_key_index_name := get_data_table_primary_key_index_name(partition_table_row.namespace_name,
                                                                    partition_table_row.partition_number,
                                                                    partition_table_row.total_partitions,
                                                                    partition_table_row.partitioning_field,
                                                                    table_start, table_end);

    schema_name := get_schema_name(partition_table_row.namespace_name);
    EXECUTE FORMAT(
        $$
            CREATE TABLE %1$I.%2$I (
                --do not use primary key b/c sort ordering is bad
                CONSTRAINT time_range CHECK(time >= %3$L AND time <= %4$L)
            ) INHERITS (%1$I.%8$I)
        $$,
        schema_name,
        table_name, table_start, table_end, partition_table_row.partitioning_field,
        partition_table_row.total_partitions, partition_table_row.partition_number,
        partition_table_row.table_name
    );

    table_oid := format('%I.%I', schema_name, table_name) :: REGCLASS;

    EXECUTE FORMAT(
        $$
            CREATE INDEX  %1$I ON %2$s ("time" desc nulls last, %3$I)
        $$,
        primary_key_index_name, table_oid, partition_table_row.partitioning_field);

    INSERT INTO data_table (table_oid, namespace_name, partition_number, total_partitions,
                            partitioning_field, start_time, end_time)
    VALUES
        (
            table_oid,
            partition_table_row.namespace_name,
            partition_table_row.partition_number,
            partition_table_row.total_partitions,
            partition_table_row.partitioning_field,
            table_start,
            table_end
        )
    RETURNING *
        INTO STRICT data_table_row;
    RETURN data_table_row;
END
$BODY$;


CREATE OR REPLACE FUNCTION _sysinternal.get_data_table(
    "time"           BIGINT,
    namespace        TEXT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS data_table LANGUAGE SQL VOLATILE AS
$BODY$
--note partitioning_field not in query
--since its unique per namespace_name, partition_number, total_partitions (by pk on partition_table)
SELECT *
FROM data_table dt
WHERE (dt.start_time <= get_data_table.time OR dt.start_time IS NULL) AND
      (dt.end_time >= get_data_table.time OR dt.end_time IS NULL) AND
      dt.namespace_name = get_data_table.namespace AND
      dt.partition_number = get_data_table.partition_number AND
      dt.total_partitions = get_data_table.total_partitions
FOR SHARE --lock this row against concurrent modifications
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.idempotent_create_partition_table_with_lock(
    namespace_name     NAME,
    partition_number   SMALLINT,
    total_partitions   SMALLINT,
    partitioning_field NAME
)
    RETURNS partition_table LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    partition_table_row  partition_table;
    partition_table_name NAME;
BEGIN
    SELECT *
    INTO partition_table_row
    FROM partition_table AS pt
    WHERE pt.namespace_name = idempotent_create_partition_table_with_lock.namespace_name AND
          pt.partition_number = idempotent_create_partition_table_with_lock.partition_number AND
          pt.total_partitions = idempotent_create_partition_table_with_lock.total_partitions AND
          pt.partitioning_field = idempotent_create_partition_table_with_lock.partitioning_field
    FOR UPDATE;

    IF partition_table_row IS NULL THEN
        partition_table_name := get_partition_table_name(namespace_name, partition_number, total_partitions);

        INSERT INTO partition_table (namespace_name, partition_number, total_partitions, partitioning_field, table_name)
        VALUES (
            namespace_name, partition_number, total_partitions, partitioning_field, partition_table_name
        )
        ON CONFLICT DO NOTHING;

        SELECT *
        INTO STRICT partition_table_row
        FROM partition_table AS pt
        WHERE pt.namespace_name = idempotent_create_partition_table_with_lock.namespace_name AND
              pt.partition_number = idempotent_create_partition_table_with_lock.partition_number AND
              pt.total_partitions = idempotent_create_partition_table_with_lock.total_partitions AND
              pt.partitioning_field = idempotent_create_partition_table_with_lock.partitioning_field
        FOR UPDATE;
    END IF;
    RETURN partition_table_row;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_or_create_data_table(
    "time"           BIGINT,
    namespace_name   NAME,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS data_table LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    data_table_row      data_table;
    partitioning_field  NAME;
    partition_table_row partition_table;
BEGIN
    --uses double-checked locking
    data_table_row := _sysinternal.get_data_table("time", namespace_name, partition_number, total_partitions);
    IF data_table_row IS NULL THEN
        partitioning_field := get_partitioning_field_name(namespace_name);
        IF partitioning_field IS NULL THEN
            RAISE EXCEPTION 'No partitioning field for namespace %', namespace_name
            USING ERRCODE = 'IO102';
        END IF;
        --get lock:
        SELECT *
        INTO partition_table_row
        FROM _sysinternal.idempotent_create_partition_table_with_lock(namespace_name, partition_number, total_partitions,
                                                                      partitioning_field);
        --recheck:
        data_table_row := _sysinternal.get_data_table("time", namespace_name, partition_number, total_partitions);
        IF data_table_row IS NULL THEN --recheck
            PERFORM _sysinternal.create_data_table("time", partition_table_row);
        END IF;

        data_table_row := _sysinternal.get_data_table("time", namespace_name, partition_number, total_partitions);
        IF data_table_row IS NULL THEN --recheck
            RAISE EXCEPTION 'Should never happen'
            USING ERRCODE = 'IO501';
        END IF;
    END IF;
    RETURN data_table_row;
END
$BODY$;


CREATE OR REPLACE FUNCTION close_data_table_end(
    table_oid REGCLASS
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    data_table_row data_table;
    max_time       BIGINT;
    table_end      BIGINT;
BEGIN
    SELECT *
    INTO STRICT data_table_row
    FROM data_table AS dt
    WHERE dt.table_oid = close_data_table_end.table_oid
    FOR UPDATE; --lock row

    EXECUTE format(
        $$
            SELECT max("time")
            FROM %s
        $$, data_table_row.table_oid)
    INTO max_time;

    IF max_time IS NULL THEN
        max_time := data_table_row.start_time;
    END IF;

    table_end := ((max_time :: BIGINT / (1e9 * 60 * 60 * 24) + 1) :: BIGINT) * (1e9 * 60 * 60 * 24) :: BIGINT - 1;

    EXECUTE FORMAT(
        $$
            ALTER TABLE %s DROP CONSTRAINT time_range
        $$,
        data_table_row.table_oid);
    EXECUTE FORMAT(
        $$
            ALTER TABLE %s ADD CONSTRAINT time_range CHECK(time >= %L AND time <= %L)
        $$,
        data_table_row.table_oid, data_table_row.start_time, table_end);

    UPDATE data_table AS dt
    SET end_time = table_end
    WHERE dt.table_oid = data_table_row.table_oid;
END
$BODY$;
