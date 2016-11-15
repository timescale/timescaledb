CREATE OR REPLACE FUNCTION get_fields_from_json(
    hypertable_name NAME
)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
    SELECT format($$((value->>'%s')::%s)$$, field_name, data_type)
    FROM (
             SELECT
                 f.name AS field_name,
                 f.data_type
             FROM field AS f
             WHERE f.hypertable_name = get_fields_from_json.hypertable_name
             ORDER BY f.name
         ) AS info
);
$BODY$;

CREATE OR REPLACE FUNCTION get_field_list(
    hypertable_name NAME
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT array_to_string(get_quoted_field_names(hypertable_name), ', ')
$BODY$;


CREATE OR REPLACE FUNCTION get_field_from_json_list(
    hypertable_name NAME
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT array_to_string(get_fields_from_json(hypertable_name), ', ')
$BODY$;


CREATE OR REPLACE FUNCTION create_temp_copy_table_one_partition(
    table_name       TEXT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    EXECUTE format(
        $$
            CREATE TEMP TABLE "%s" (
                hypertable_name name NOT NULL,
                time BIGINT NOT NULL,
                value jsonb
            )  ON COMMIT DROP
        $$, table_name);

    RETURN table_name;
END
$BODY$;

--creates fields from project_series, not namespaces
CREATE OR REPLACE FUNCTION insert_data_one_partition(
    copy_table_oid   REGCLASS,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    chunk_replica_node_row  chunk_replica;
    distinct_table_oid      REGCLASS;
    time_point              BIGINT;
    hypertable_point        NAME;
BEGIN
    time_point := 1;

    EXECUTE format(
        $$
            SELECT "time", hypertable_name FROM %s ORDER BY hypertable_name LIMIT 1
        $$,
        copy_table_oid)
    INTO time_point, hypertable_point;

    WHILE time_point IS NOT NULL LOOP
        SELECT *
        INTO distinct_table_oid
        FROM get_distinct_local_table_oid(hypertable_point);

        SELECT *
        INTO data_table_row
        FROM get_or_create_data_table(time_point, hypertable_point,
                                      partition_number, total_partitions);

        BEGIN
            EXECUTE format(
                $$
                    WITH selected AS
                    (
                        DELETE FROM %2$s
                        WHERE ("time" >= %3$L OR  %3$L IS NULL) and ("time" <= %4$L OR %4$L IS NULL)
                              AND hypertable_name = %5$L
                        RETURNING *
                    ),
                    distinct_field AS (
                        SELECT name
                        FROM field
                        WHERE hypertable_name = %5$L  AND is_distinct = TRUE
                    ),
                    insert_distinct AS (
                        INSERT INTO  %6$s as distinct_table
                            SELECT distinct_field.name, value->>distinct_field.name, max(time)
                            FROM  distinct_field
                            CROSS JOIN selected
                            WHERE value ? distinct_field.name
                            GROUP BY distinct_field.name, (value->>distinct_field.name)
                            ON CONFLICT (field, value)
                                --DO NOTHING
                                DO UPDATE
                                SET last_time_approx = EXCLUDED.last_time_approx + (1e9::bigint * 60*60*24::bigint)
                                WHERE EXCLUDED.last_time_approx > distinct_table.last_time_approx
                                --DO UPDATE SET last_time_approx = EXCLUDED.last_time_approx
                    )
                    INSERT INTO %1$s (time, %7$s) SELECT time, %8$s FROM selected;
                $$, data_table_row.table_oid, copy_table_oid, data_table_row.start_time,
                data_table_row.end_time, hypertable_point, distinct_table_oid,
                get_field_list(hypertable_point),
                get_field_from_json_list(hypertable_point))
            USING data_table_row;

            EXECUTE format(
                $$
                    SELECT "time", hypertable_name FROM %s ORDER BY hypertable_name LIMIT 1
                $$,
                copy_table_oid)
            INTO time_point, hypertable_point;

            EXCEPTION WHEN deadlock_detected THEN
            --do nothing, rerun loop (deadlock can be caused by concurrent updates to distinct table)
            --TODO: try to get rid of this by ordering the insert
        END;
    END LOOP;
END
$BODY$;







