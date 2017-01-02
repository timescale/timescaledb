-- Get a comma-separated list of fields in a hypertable.
CREATE OR REPLACE FUNCTION get_field_list(
    hypertable_name NAME
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT array_to_string(get_quoted_field_names(hypertable_name), ', ')
$BODY$;

-- Creates a temporary table with the same structure as a given hypertable.
-- This can be used for bulk inserts.
CREATE OR REPLACE FUNCTION create_temp_copy_table(
    hypertable_name NAME,
    table_name      TEXT
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    root_schema NAME;
    root_table  NAME;
BEGIN
    EXECUTE format(
        $$
            SELECT h.root_schema_name, h.root_table_name
            FROM hypertable h
            WHERE h.name = %L
            LIMIT 1
        $$, hypertable_name)
    INTO root_schema, root_table;
    EXECUTE format(
        $$
            CREATE TEMP TABLE "%s" ON COMMIT DROP AS (
                SELECT * FROM %I.%I WHERE FALSE
            )
        $$, table_name, root_schema, root_table);

    RETURN table_name;
END
$BODY$;

-- Gets the partition ID of a given epoch and data row.
CREATE OR REPLACE FUNCTION get_partition_for_epoch_row(
    epoch           partition_epoch,
    copy_record     anyelement,
    copy_table_name TEXT
)
    RETURNS partition LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    partition_row partition;
BEGIN
    EXECUTE format(
        $$
            SELECT  p.*
            FROM partition p
            WHERE p.epoch_id = %L AND
            %s((SELECT row.%I FROM (SELECT (%L::%s).*) as row), %L)
            BETWEEN p.keyspace_start AND p.keyspace_end
        $$,
            epoch.id, epoch.partitioning_func,
            epoch.partitioning_field,
            copy_record, copy_table_name, epoch.partitioning_mod)
    INTO STRICT partition_row;

    RETURN partition_row;
END
$BODY$;

-- Gets the time value of a temp table row.
CREATE OR REPLACE FUNCTION get_time_from_copy_row(
    field_name      NAME,
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
            SELECT row.%I FROM (SELECT (%L::%s).*) as row LIMIT 1
        $$, field_name, copy_record, copy_table_name)
    INTO STRICT t;

    RETURN t;
END
$BODY$;

-- Inserts rows from a temporary table into correct hypertable child tables.
CREATE OR REPLACE FUNCTION insert_data(
    hypertable_name NAME,
    copy_table_oid  REGCLASS
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    crn_record            RECORD;
    distinct_table_oid    REGCLASS;
    time_point            BIGINT;
    time_field_name_point NAME;
    partition_id          INT;
    distinct_field        TEXT;
    distinct_clauses      TEXT;
    distinct_clause_idx   INT;
BEGIN
    time_point := 1;
    EXECUTE format(
        $$
            SELECT get_time_from_copy_row(h.time_field_name, ct, '%1$s'), h.time_field_name, p.id
            FROM ONLY %1$s ct
            LEFT JOIN hypertable h ON (h.NAME = %2$L)
            LEFT JOIN partition_epoch pe ON (
              pe.hypertable_name = %2$L AND
              (pe.start_time <= (SELECT get_time_from_copy_row(h.time_field_name, ct, '%1$s'))::bigint OR pe.start_time IS NULL) AND
              (pe.end_time   >= (SELECT get_time_from_copy_row(h.time_field_name, ct, '%1$s'))::bigint OR pe.end_time IS NULL)
            )
            LEFT JOIN get_partition_for_epoch_row(pe, ct, '%1$s') AS p ON(true)
            LIMIT 1
        $$, copy_table_oid, hypertable_name)
    INTO STRICT time_point, time_field_name_point, partition_id;
    IF time_point IS NOT NULL AND partition_id IS NULL THEN
        RAISE EXCEPTION 'Should never happen: could not find partition for insert'
        USING ERRCODE = 'IO501';
    END IF;

    WHILE time_point IS NOT NULL LOOP
        FOR crn_record IN
        SELECT
            crn.database_name,
            crn.schema_name,
            crn.table_name,
            c.start_time,
            c.end_time,
            pr.hypertable_name,
            pr.replica_id
        FROM get_or_create_chunk(partition_id, time_point) c
        INNER JOIN chunk_replica_node crn ON (crn.chunk_id = c.id)
        INNER JOIN partition_replica pr ON (pr.id = crn.partition_replica_id)
        LOOP
            distinct_clauses := '';
            distinct_clause_idx := 0;

            SELECT *
            INTO distinct_table_oid
            FROM get_distinct_table_oid(hypertable_name, crn_record.replica_id, crn_record.database_name);

            FOR distinct_field IN
            SELECT f.name
            FROM field as f
            WHERE f.is_distinct = TRUE AND f.hypertable_name = insert_data.hypertable_name
            ORDER BY f.name
            LOOP
                distinct_clauses := distinct_clauses || ',' || format(
                    $$
                    insert_distinct_%3$s AS (
                         INSERT INTO  %1$s as distinct_table
                             SELECT DISTINCT %2$L, selected.%2$I as value
                             FROM selected
                             ORDER BY value
                             ON CONFLICT
                                 DO NOTHING
                     )
                     $$, distinct_table_oid, distinct_field, distinct_clause_idx);
                distinct_clause_idx := distinct_clause_idx + 1;
            END LOOP;

            EXECUTE format(
                $$
              WITH selected AS
              (
                  DELETE FROM ONLY %2$s
                  WHERE (%7$I >= %3$L OR %3$L IS NULL) and (%7$I <= %4$L OR %4$L IS NULL)
                  RETURNING *
              )%5$s
              INSERT INTO %1$s (%6$s) SELECT %6$s FROM selected;
          $$,
                format('%I.%I', crn_record.schema_name, crn_record.table_name) :: REGCLASS,
                copy_table_oid, crn_record.start_time, crn_record.end_time,
                distinct_clauses,
                get_field_list(hypertable_name),
                time_field_name_point);
        END LOOP;

        EXECUTE format(
            $$
                SELECT get_time_from_copy_row(h.time_field_name, ct, '%1$s'), h.time_field_name, p.id
                FROM ONLY %1$s ct
                LEFT JOIN hypertable h ON (h.NAME = %2$L)
                LEFT JOIN partition_epoch pe ON (
                  pe.hypertable_name = %2$L AND
                  (pe.start_time <= (SELECT get_time_from_copy_row(h.time_field_name, ct, '%1$s'))::bigint OR pe.start_time IS NULL) AND
                  (pe.end_time   >= (SELECT get_time_from_copy_row(h.time_field_name, ct, '%1$s'))::bigint OR pe.end_time IS NULL)
                )
                LEFT JOIN get_partition_for_epoch_row(pe, ct, '%1$s') AS p ON(true)
                LIMIT 1
            $$, copy_table_oid, hypertable_name)
        INTO time_point, time_field_name_point, partition_id;

        IF time_point IS NOT NULL AND partition_id IS NULL THEN
            RAISE EXCEPTION 'Should never happen: could not find partition for insert'
            USING ERRCODE = 'IO501';
        END IF;
    END LOOP;
END
$BODY$;
