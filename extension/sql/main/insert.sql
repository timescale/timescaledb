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


CREATE OR REPLACE FUNCTION create_temp_copy_table(
    table_name TEXT
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    EXECUTE format(
        $$
            CREATE TEMP TABLE "%s" (
                value jsonb
            )  ON COMMIT DROP
        $$, table_name);

    RETURN table_name;
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
    crn_record         RECORD;
    distinct_table_oid REGCLASS;
    time_point         BIGINT;
    time_field_name_point NAME;
    partition_id       INT;
BEGIN
    time_point := 1;
    EXECUTE format(
        $$
            SELECT value->>h.time_field_name, h.time_field_name, p.id
            FROM %1$s ct
            LEFT JOIN hypertable h ON (h.NAME = %2$L)
            LEFT JOIN partition_epoch pe ON (
              pe.hypertable_name = %2$L AND
              (pe.start_time <= (value->>h.time_field_name)::bigint OR pe.start_time IS NULL) AND
              (pe.end_time   >= (value->>h.time_field_name)::bigint OR pe.end_time IS NULL)
            )
            LEFT JOIN  get_partition_for_epoch(pe, value->>pe.partitioning_field) AS p ON(true)
            LIMIT 1
        $$, copy_table_oid, hypertable_name)
    INTO time_point, time_field_name_point, partition_id;
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
            SELECT *
            INTO distinct_table_oid
            FROM get_distinct_table_oid(hypertable_name, crn_record.replica_id, crn_record.database_name);

            EXECUTE format(
                $$
              WITH selected AS
              (
                  DELETE FROM %2$s
                  WHERE ((value->>%8$L)::bigint >= %3$L OR  %3$L IS NULL) and ((value->>%8$L)::bigint <= %4$L OR %4$L IS NULL)
                  RETURNING *
              ),
              distinct_field AS (
                  SELECT name
                  FROM field
                  WHERE is_distinct = TRUE
              ),
              insert_distinct AS (
                  INSERT INTO  %5$s as distinct_table
                      SELECT distinct_field.name, value->>distinct_field.name
                      FROM  distinct_field
                      CROSS JOIN selected
                      WHERE value ? distinct_field.name
                      GROUP BY distinct_field.name, (value->>distinct_field.name)
                      ON CONFLICT
                          DO NOTHING
              )
              INSERT INTO %1$s (%6$s) SELECT %7$s FROM selected;
          $$,
                format('%I.%I', crn_record.schema_name, crn_record.table_name) :: REGCLASS,
                copy_table_oid, crn_record.start_time, crn_record.end_time,
                distinct_table_oid,
                get_field_list(hypertable_name),
                get_field_from_json_list(hypertable_name),
                time_field_name_point);
        END LOOP;

        EXECUTE format(
            $$
                SELECT value->>h.time_field_name, h.time_field_name, p.id
                FROM %1$s ct
                LEFT JOIN hypertable h ON (h.NAME = %2$L)
                LEFT JOIN partition_epoch pe ON (
                  pe.hypertable_name = %2$L AND
                  (pe.start_time <= (value->>h.time_field_name)::bigint OR pe.start_time IS NULL) AND
                  (pe.end_time   >= (value->>h.time_field_name)::bigint OR pe.end_time IS NULL)
                )
                LEFT JOIN  get_partition_for_epoch(pe, value->>pe.partitioning_field) AS p ON(true)
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
