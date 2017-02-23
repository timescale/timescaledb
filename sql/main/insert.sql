-- This file contains functions that aid in inserting data into a hypertable.

-- Get a comma-separated list of columns in a hypertable.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_column_list(
    hypertable_id INTEGER
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT array_to_string(_iobeamdb_internal.get_quoted_column_names(hypertable_id), ', ')
$BODY$;

-- Gets the partition ID of a given epoch and data row.
--
-- epoch - The epoch whose partition ID we want
-- copy_record - Record/row from a table
-- copy_table_name - Name of the relation to cast the record to.
CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_partition_for_epoch_row(
    epoch           _iobeamdb_catalog.partition_epoch,
    copy_record     anyelement,
    copy_table_name TEXT
)
    RETURNS _iobeamdb_catalog.partition LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    partition_row _iobeamdb_catalog.partition;
BEGIN
    IF epoch.partitioning_func IS NULL THEN
        SELECT  p.*
        FROM _iobeamdb_catalog.partition p
        WHERE p.epoch_id = epoch.id
        INTO STRICT partition_row;
    ELSE
        EXECUTE format(
            $$
                SELECT  p.*
                FROM _iobeamdb_catalog.partition p
                WHERE p.epoch_id = %L AND
                %I.%I((SELECT row.%I FROM (SELECT (%L::%s).*) as row)::TEXT, %L)
                BETWEEN p.keyspace_start AND p.keyspace_end
            $$,
                epoch.id, epoch.partitioning_func_schema, epoch.partitioning_func,
                epoch.partitioning_column,
                copy_record, copy_table_name, epoch.partitioning_mod)
        INTO STRICT partition_row;
    END IF;
    RETURN partition_row;
END
$BODY$;

-- Gets the value of the time column from a given row.
--
-- column_name - Name of time column to fetch
-- column_type - Type of the time record
-- copy_record - Record/row from a table
-- copy_table_name - Name of the relation to cast the record to
CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_time_column_from_record(
    column_name      NAME,
    column_type      REGTYPE,
    copy_record     anyelement,
    copy_table_name TEXT
)
    RETURNS bigint LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    t bigint;
BEGIN
    EXECUTE format(
        $$
            SELECT %s FROM (SELECT (%L::%s).*) as row LIMIT 1
        $$, _iobeamdb_internal.extract_time_sql(format('row.%I', column_name), column_type), copy_record, copy_table_name)
    INTO STRICT t;

    RETURN t;
END
$BODY$;

-- Inserts rows from a (temporary) table into correct hypertable child tables.
--
-- In typical use case, the copy_table_oid is the OID of a hypertable's main
-- table. This allows users to use normal SQL INSERT calls on the main table,
-- and a trigger that executes after the statement will call this function to
-- place the data appropriately.
--
-- hypertable_id - ID of the hypertable the data belongs to
-- copy_table_oid - OID of the table to fetch rows from
CREATE OR REPLACE FUNCTION _iobeamdb_internal.insert_data(
    hypertable_id   INTEGER,
    copy_table_oid  REGCLASS
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    point_record_query_sql      TEXT;
    point_record                RECORD;
    chunk_row                   _iobeamdb_catalog.chunk;
    chunk_id                    INT;
    crn_record                  RECORD;
    hypertable_row              RECORD;
    partition_constraint_where_clause   TEXT = '';
    column_list TEXT = '';
    insert_sql TEXT ='';
BEGIN
    --This guard protects against calling insert_data() twice in the same transaction,
    --which might otherwise cause a deadlock in case the second insert_data() involves a chunk
    --that was inserted into in the first call to insert_data().
    --This is a temporary safe guard that should ideally be removed once chunk management
    --has been refactored and improved to avoid such deadlocks.
    --NOTE: In its current form, this safe guard unfortunately prohibits transactions
    --involving INSERTs on two different hypertables.
    IF current_setting('io.insert_data_guard', true) = 'on' THEN
        RAISE EXCEPTION 'insert_data() can only be called once per transaction';
    END IF;

    PERFORM set_config('io.insert_data_guard', 'on', true);
    PERFORM set_config('io.ignore_delete_in_trigger', 'true', true);

    SELECT * INTO hypertable_row FROM _iobeamdb_catalog.hypertable h WHERE h.id = hypertable_id;
    column_list :=  _iobeamdb_internal.get_column_list(hypertable_id);

    point_record_query_sql := format(
        $$
            SELECT %3$s AS time,
                   p.id AS partition_id, p.keyspace_start, p.keyspace_end,
                   pe.partitioning_func_schema, pe.partitioning_func, pe.partitioning_column, pe.partitioning_mod
            FROM (SELECT * FROM ONLY %1$s LIMIT 1) ct
            LEFT JOIN _iobeamdb_catalog.partition_epoch pe ON (
              pe.hypertable_id = %2$L AND
              (pe.start_time <= %3$s OR pe.start_time IS NULL) AND
              (pe.end_time   >= %3$s OR pe.end_time IS NULL)
            )
            LEFT JOIN _iobeamdb_internal.get_partition_for_epoch_row(pe, ct::%1$s, '%1$s') AS p ON(true)
        $$, copy_table_oid, hypertable_id, 
        _iobeamdb_internal.extract_time_sql(format('ct.%I', hypertable_row.time_column_name), hypertable_row.time_column_type));

    --can be inserting empty set so not strict.
    EXECUTE point_record_query_sql
    INTO point_record;

    IF point_record.time IS NOT NULL AND point_record.partition_id IS NULL THEN
        RAISE EXCEPTION 'Should never happen: could not find partition for insert'
        USING ERRCODE = 'IO501';
    END IF;

    --Create a temp table to collect all the chunks we insert into. We might
    --need to close the chunks at the end of the transaction.
    CREATE TEMP TABLE IF NOT EXISTS insert_chunks(LIKE _iobeamdb_catalog.chunk) ON COMMIT DROP;

    --We need to truncate the table if it already existed due to calling this
    --function twice in a single transaction.
    TRUNCATE TABLE insert_chunks;

    WHILE point_record.time IS NOT NULL LOOP
        --Get a chunk with SHARE lock
        INSERT INTO insert_chunks 
        SELECT * FROM get_or_create_chunk(point_record.partition_id, point_record.time, TRUE) 
        RETURNING * INTO chunk_row;

        IF point_record.partitioning_column IS NOT NULL THEN
            --if we are inserting across more than one partition,
            --construct a WHERE clause constraint that SELECTs only
            --values from copy table that match the current partition
            partition_constraint_where_clause := format(
                $$
                WHERE (%1$I >= %2$s OR %2$s IS NULL) AND (%1$I <= %3$s OR %3$s IS NULL) AND
                  (%4$I.%5$I(%6$I::TEXT, %7$L) BETWEEN %8$L AND %9$L)
                $$,
                hypertable_row.time_column_name,
                _iobeamdb_internal.time_literal_sql(chunk_row.start_time, hypertable_row.time_column_type),
                _iobeamdb_internal.time_literal_sql(chunk_row.end_time, hypertable_row.time_column_type),
                point_record.partitioning_func_schema,
                point_record.partitioning_func,
                point_record.partitioning_column,
                point_record.partitioning_mod,
                point_record.keyspace_start,
                point_record.keyspace_end
            );
        END IF;

        --Do insert on all chunk replicas
        SELECT string_agg(insert_stmt, ',')
        INTO insert_sql
        FROM (
            SELECT format('i_%s AS (INSERT INTO %I.%I (%s) SELECT * FROM selected)',
                row_number() OVER(), crn.schema_name, crn.table_name, column_list) insert_stmt
            FROM _iobeamdb_catalog.chunk_replica_node crn
            WHERE (crn.chunk_id = chunk_row.id)
        ) AS parts;

        EXECUTE format(
            $$
            WITH selected AS
            (
                DELETE FROM ONLY %1$s %2$s  
                RETURNING %4$s
            ), 
            %3$s
            SELECT 1
            $$,
                copy_table_oid,
                partition_constraint_where_clause,
                insert_sql,
                column_list
            );

        EXECUTE point_record_query_sql
        INTO point_record;

        IF point_record.time IS NOT NULL AND point_record.partition_id IS NULL THEN
            RAISE EXCEPTION 'Should never happen: could not find partition for insert'
            USING ERRCODE = 'IO501';
        END IF;
    END LOOP;

    --Loop through all open chunks that were inserted into, closing
    --if needed. Do it in ID order to avoid deadlocks.
    FOR chunk_id IN
    SELECT c.id FROM insert_chunks cl
    INNER JOIN _iobeamdb_catalog.chunk c ON cl.id = c.id
    WHERE c.end_time IS NULL ORDER BY cl.id DESC
    LOOP
        PERFORM _iobeamdb_internal.close_chunk_if_needed(chunk_id);
    END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION _iobeamdb_internal.insert_trigger_on_copy_table()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            SELECT _iobeamdb_internal.insert_data(%1$L, %2$L)
        $$,  TG_ARGV[0], TG_RELID);
    RETURN NEW;
END
$BODY$;
