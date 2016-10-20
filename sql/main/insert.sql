CREATE OR REPLACE FUNCTION get_field_type_from_ns_type(ns_type TEXT)
    RETURNS REGTYPE LANGUAGE SQL STABLE AS $$
SELECT CASE ns_type
       WHEN 'DOUBLE' THEN
           'DOUBLE PRECISION' :: REGTYPE
       WHEN 'STRING' THEN
           'TEXT' :: REGTYPE
       WHEN 'BOOLEAN' THEN
           'BOOLEAN' :: REGTYPE
       WHEN 'LONG' THEN
           'BIGINT' :: REGTYPE
       END;
$$;

--create data tables for appropriate time values
--tables are created open-ended in one direction (either start_time or end_time is NULL)
--that way, tables grow in some time dir.
--Tables are always created adjacent to existing tables. So, tables
--will never be disjoint in terms of time. Therefore you will have:
--     <---current open ended start_time table --| existing closed tables | -- current open ended end_time table --->
-- tables start and end times are aligned at day boundaries.
CREATE OR REPLACE FUNCTION create_data_table(
    "time"           BIGINT,
    project_id       INT,
    namespace        TEXT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS data_tables LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    data_table_row       data_tables;
    table_start          BIGINT;
    table_end            BIGINT;
    table_name           TEXT;
    partition_table_name TEXT;
    partitioning_field   TEXT;
BEGIN
    SELECT *
    INTO data_table_row
    FROM data_tables AS dt
    WHERE dt.end_time < create_data_table."time" AND
          dt.project_id = create_data_table.project_id AND
          dt.namespace = create_data_table.namespace AND
          dt.replica_no = create_data_table.replica_no AND
          dt."partition" = create_data_table.partition_number AND
          dt.total_partitions = create_data_table.total_partitions
    ORDER BY end_time DESC
    LIMIT 1;

    IF NOT FOUND THEN
        SELECT *
        INTO data_table_row
        FROM data_tables AS dt
        WHERE dt.start_time > create_data_table."time" AND
              dt.project_id = create_data_table.project_id AND
              dt.namespace = create_data_table.namespace AND
              dt.replica_no = create_data_table.replica_no AND
              dt."partition" = create_data_table.partition_number AND
              dt.total_partitions = create_data_table.total_partitions
        ORDER BY start_time ASC
        LIMIT 1;

        IF NOT FOUND THEN
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

    IF table_start IS NOT NULL THEN
        table_name := format('%s_%s_%s_%s_%s_%s', project_id, namespace, replica_no, total_partitions, partition_number,
                             table_start / 1e9 :: BIGINT);
    ELSE
        table_name := format('%s_%s_%s_%s_%s_%s', project_id, namespace, replica_no, total_partitions, partition_number,
                             table_end / 1e9 :: BIGINT);
    END IF;

    partition_table_name := get_partition_table_name(project_id, namespace, replica_no, partition_number,
                                                     total_partitions);
    partitioning_field := get_partitioning_field(project_id, namespace);

    EXECUTE FORMAT(
        $$
            CREATE TABLE "%s" (
                --do not use primary key b/c sort ordering is bad
                CONSTRAINT time_range CHECK(time >= %L AND time <= %L),
                CONSTRAINT partition CHECK(get_partition_for_key(%s, %L) = %L)
            ) INHERITS ("%s")
        $$,
        table_name, table_start, table_end, partitioning_field, total_partitions, partition_number,
        partition_table_name);

    EXECUTE FORMAT(
        $$
            CREATE INDEX  "%1$s_pidx" ON "%1$s" ("time" desc nulls last, %2$s)
        $$,
        table_name, partitioning_field);

    INSERT INTO data_tables (table_name, project_id, namespace, replica_no, "partition", total_partitions, start_time, end_time)
    VALUES
        (
            FORMAT('"%s"', table_name),
            project_id,
            namespace,
            replica_no,
            partition_number,
            total_partitions,
            table_start,
            table_end
        )
    RETURNING *
        INTO STRICT data_table_row;

    PERFORM create_field_on_data_table(data_table_row, field_name, is_distinct, is_partition_key, value_type, idx_types)
    FROM (
             SELECT DISTINCT
                 field AS field_name,
                 is_distinct,
                 is_partition_key,
                 value_type,
                 idx_types
             FROM "cluster".project_field AS pf
             WHERE pf.project_id = create_data_table.project_id AND
                   pf.namespace = create_data_table.namespace
         ) AS a;

    RETURN data_table_row;
END
$BODY$;


CREATE OR REPLACE FUNCTION get_distinct_table(
    project_id BIGINT,
    namespace  TEXT,
    replica_no SMALLINT
)
    RETURNS distinct_tables LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    distinct_table_row distinct_tables;
    table_name         TEXT;
BEGIN
    SELECT *
    INTO distinct_table_row
    FROM distinct_tables AS dt
    WHERE dt.project_id = get_distinct_table.project_id AND
          dt.namespace = get_distinct_table.namespace AND
          dt.replica_no = get_distinct_table.replica_no;

    IF NOT FOUND THEN
        table_name := format('%s_%s_%s_distinct', project_id, namespace, replica_no);

        --we need to do this in a transaction so that the table are created by the time
        --they are registered in the global registry
        --dblink is the only way to hack a transaction commit within a postgres function
        PERFORM dblink_exec('local', format(
            $DBL$
                DO $$
                BEGIN
                    CREATE TABLE IF NOT EXISTS cluster."%1$s" (
                        field TEXT,
                        value TEXT,
                        last_time_approx BIGINT,
                        PRIMARY KEY(field, value)
                    );

                    EXCEPTION
                    WHEN unique_violation OR duplicate_table OR duplicate_object THEN
                       --ignore errors of simultaneous table creation due to the global table registry creating cluster tables; conflicting with this.
                       --the gtr does not do a lock table, thus the lock above is insufficient.
                       NULL;
                END$$;
                CREATE TABLE IF NOT EXISTS public."%1$s"
                  (PRIMARY KEY(field, value)) INHERITS(cluster."%1$s");
            $DBL$,
            table_name));

        INSERT INTO distinct_tables (table_name, project_id, namespace, replica_no)
        VALUES (
            FORMAT('"%s"', table_name),
            get_distinct_table.project_id,
            get_distinct_table.namespace,
            get_distinct_table.replica_no)
        ON CONFLICT DO NOTHING;

        SELECT *
        INTO STRICT distinct_table_row
        FROM distinct_tables AS dt
        WHERE dt.project_id = get_distinct_table.project_id AND
              dt.namespace = get_distinct_table.namespace AND
              dt.replica_no = get_distinct_table.replica_no;

        PERFORM register_global_table(table_name, table_name);
    END IF;
    RETURN distinct_table_row;
END
$BODY$;


CREATE OR REPLACE FUNCTION get_data_table(
    "time"           BIGINT,
    project_id       INT,
    namespace        TEXT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS data_tables LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    data_table_row data_tables;
BEGIN
    SELECT *
    INTO data_table_row
    FROM data_tables dt
    WHERE (dt.start_time <= get_data_table.time OR dt.start_time IS NULL) AND
          (dt.end_time >= get_data_table.time OR dt.end_time IS NULL) AND
          dt.project_id = get_data_table.project_id AND
          dt.namespace = get_data_table.namespace AND
          dt.replica_no = get_data_table.replica_no AND
          dt."partition" = partition_number;

    IF NOT FOUND THEN
        RETURN create_data_table(time, project_id, namespace, replica_no,
                                 partition_number, total_partitions);
    ELSE
        RETURN data_table_row;
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION close_data_table_end(data_table_row data_tables)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    max_time  BIGINT;
    table_end BIGINT;
BEGIN
    EXECUTE format($$ SELECT max("time") FROM %s $$, data_table_row.table_name)
    INTO max_time;

    IF max_time IS NULL THEN
        max_time := data_table_row.start_time;
    END IF;

    table_end := ((max_time :: BIGINT / (1e9 * 60 * 60 * 24) + 1) :: BIGINT) * (1e9 * 60 * 60 * 24) :: BIGINT - 1;

    EXECUTE FORMAT(
        $$
            ALTER TABLE %s DROP CONSTRAINT time_range
        $$,
        data_table_row.table_name);
    EXECUTE FORMAT(
        $$
            ALTER TABLE %s ADD  CONSTRAINT time_range CHECK(time >= %L AND time <= %L)
        $$,
        data_table_row.table_name, data_table_row.start_time, table_end);

    UPDATE data_tables
    SET end_time = table_end
    WHERE table_name = data_table_row.table_name;
END
$BODY$;


CREATE OR REPLACE FUNCTION create_index(
    table_oid  REGCLASS,
    field_name TEXT,
    index_type field_index_type,
    field_type REGTYPE
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    index_name        TEXT;
    index_name_offset INT := 0;
    suffix            TEXT;
    table_name        TEXT;
BEGIN

    SELECT relname
    INTO table_name
    FROM pg_class
    WHERE oid = table_oid;

    LOOP
        --TODO: change logic here to use a sequence instead of index_name_offset
        --get rid of table_name too in that way, use idx_seq_field-time
        IF index_type = 'TIME-VALUE' THEN
            index_name := format('%s-time-%s', table_name, field_name);
        ELSIF index_type = 'VALUE-TIME' THEN
            index_name := format('%s-%s-time', table_name, field_name);
        ELSE
            index_name := format('%s-%s', table_name, field_name);
        END IF;

        suffix := format('-%s', index_name_offset);
        index_name := substring(index_name FROM 1 FOR (63 - char_length(suffix)))
                      || suffix;

        BEGIN
            IF index_type = 'TIME-VALUE' THEN
                EXECUTE format(
                    $$
                        CREATE INDEX "%4$s" ON %1$s
                        USING BTREE(time DESC NULLS LAST, "%2$s")
                        WHERE %2$I IS NOT NULL
                    $$,
                    table_oid, field_name, field_type, index_name);
            ELSIF index_type = 'VALUE-TIME' THEN
                EXECUTE format(
                    $$
                        CREATE INDEX "%4$s" ON %1$s
                        USING BTREE( "%2$s", time DESC NULLS LAST) --WITH (fillfactor=50)
                        WHERE %2$I IS NOT NULL
                    $$,
                    table_oid, field_name, field_type, index_name);
            END IF;

            INSERT INTO data_field_idx (table_name, field_name, idx_name, idx_type)
            VALUES (table_oid, field_name, index_name, index_type);

            RETURN;

            EXCEPTION
            WHEN duplicate_table THEN
            --ignore will redo loop
        END;
        index_name_offset := index_name_offset + 1;
    END LOOP;
END
$BODY$;


CREATE OR REPLACE FUNCTION create_field_from_definition(
    project_id      BIGINT,
    namespace       TEXT,
    replica_no      SMALLINT,
    field           TEXT,
    is_distinct     BOOLEAN,
    is_partitioning BOOLEAN,
    data_type_oid   REGTYPE,
    index_type      field_index_type []
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
BEGIN
    --add field to cluster table.
    BEGIN
        EXECUTE format(
            $$ ALTER TABLE cluster.%1$I ADD COLUMN %2$I %3$s DEFAULT NULL $$,
            get_cluster_table_name(project_id, namespace, replica_no),
            field, data_type_oid);
        EXCEPTION
        WHEN duplicate_column THEN --ignore since this function should be idempotent
    END;

    --initiate all data_fields
    PERFORM create_field_on_data_table(dt, field, is_distinct, is_partitioning,
                                       data_type_oid, index_type)
    FROM data_tables AS dt
    WHERE dt.project_id = create_field_from_definition.project_id AND
          dt.namespace = create_field_from_definition.namespace AND
          dt.replica_no = create_field_from_definition.replica_no;
END
$BODY$;


CREATE OR REPLACE FUNCTION create_field_on_data_table(
    data_table_row  data_tables,
    field_name      TEXT,
    is_distinct     BOOLEAN,
    is_partitioning BOOLEAN,
    field_type      REGTYPE,
    index_types     field_index_type []
)
    RETURNS data_fields LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    data_field_rows data_fields;
    index_type      field_index_type;
BEGIN
    INSERT INTO data_fields (table_name, field_name, field_type, is_distinct, is_partitioning)
    VALUES (data_table_row.table_name, field_name, field_type, is_distinct, is_partitioning)
    ON CONFLICT DO NOTHING
    RETURNING *
        INTO data_field_rows;

    IF data_field_rows IS NULL THEN
        RAISE WARNING 'Create field CANCELLED (already exists) indexes % on data table %',
        field_name, data_table_row.table_name;
        RETURN NULL; --was already added
    END IF;

    RAISE NOTICE 'Create field indexes % on data table %',
    field_name, data_table_row.table_name;

    FOR index_type IN SELECT unnest(index_types) LOOP
        PERFORM create_index(data_table_row.table_name, field_name, index_type, field_type);
    END LOOP;
    RETURN data_field_rows;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_partitioning_field(
    project_id INT,
    namespace  TEXT
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT field
FROM cluster.project_field AS pf
WHERE pf.project_id = get_partitioning_field.project_id AND
      pf.namespace = get_partitioning_field.namespace AND
      is_partition_key = TRUE;
$BODY$;


CREATE OR REPLACE FUNCTION get_fields(
    project_id INT,
    namespace  TEXT
)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
    SELECT DISTINCT format('%I', field) AS field_identifier
    FROM cluster.project_field AS pf
    WHERE pf.project_id = get_fields.project_id AND
          pf.namespace = get_fields.namespace
    ORDER BY field_identifier
);
$BODY$;

CREATE OR REPLACE FUNCTION get_fields_from_json(
    project_id INT,
    namespace  TEXT
)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
           SELECT format($$((value->>'%s')::%s)$$, field, value_type)
           FROM (
                    SELECT DISTINCT
                        field,
                        value_type
                    FROM cluster.project_field AS pf
                    WHERE pf.project_id = get_fields_from_json.project_id AND
                          pf.namespace = get_fields_from_json.namespace
                    ORDER BY field
                ) AS a
       ) AS info;
$BODY$;


CREATE OR REPLACE FUNCTION get_field_type(
    project_id BIGINT,
    namespace  TEXT,
    field_name TEXT)
    RETURNS REGTYPE LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
DECLARE
    field_oid REGTYPE;
BEGIN
    SELECT DISTINCT value_type
    INTO STRICT field_oid
    FROM cluster.project_field AS pf
    WHERE
        pf.project_id = get_field_type.project_id AND
        pf.namespace = get_field_type.namespace AND
        pf.field = get_field_type.field_name;

    RETURN field_oid;

    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'Field type not found. Field name: % project_id: % namespace: %',
        field_name, project_id, namespace;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_fields_def(
    project_id INT,
    namespace  TEXT
)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
           SELECT format($$ %I %s $$, field, value_type)
           FROM (
                    SELECT DISTINCT
                        field,
                        value_type
                    FROM cluster.project_field AS pf
                    WHERE pf.project_id = get_fields_def.project_id AND
                          pf.namespace = get_fields_def.namespace
                    ORDER BY field
                ) AS a
       ) AS info;
$BODY$;


CREATE OR REPLACE FUNCTION get_field_def(
    project_id BIGINT,
    namespace  TEXT,
    field_name TEXT
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT format($$ %I %s $$, field_name, get_field_type(project_id, namespace, field_name))
$BODY$;


CREATE OR REPLACE FUNCTION get_field_list(
    project_id INT,
    namespace  TEXT
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT array_to_string(get_fields(project_id, namespace), ', ')
$BODY$;


CREATE OR REPLACE FUNCTION get_field_from_json_list(
    project_id INT,
    namespace  TEXT
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT array_to_string(get_fields_from_json(project_id, namespace), ', ')
$BODY$;



CREATE OR REPLACE FUNCTION get_master_table_name(
    project_id BIGINT,
    namespace  TEXT,
    replica_no SMALLINT
)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('%s_%s_%s_master', project_id, namespace, replica_no);
$BODY$;

CREATE OR REPLACE FUNCTION get_partition_table_name(
    project_id       INT,
    namespace        TEXT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('%s_%s_%s_%s_partition', project_id, namespace, replica_no, partition_number);
$BODY$;

CREATE OR REPLACE FUNCTION get_temp_copy_table_name(
    project_id       INT,
    namespace        TEXT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('%s_%s_%s_%s_copy_t', project_id, namespace, replica_no, partition_number);
$BODY$;


CREATE OR REPLACE FUNCTION create_partition_table(
    project_id       INT,
    namespace        TEXT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    master_table_name TEXT;
    table_name        TEXT;
BEGIN
    master_table_name := get_master_table_name(project_id, namespace, replica_no);
    table_name := get_partition_table_name(project_id, namespace, replica_no, partition_number, total_partitions);

    EXECUTE format($$ CREATE TABLE IF NOT EXISTS "%s" () INHERITS("%s") $$, table_name, master_table_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION create_temp_copy_table_one_partition(
    table_name       TEXT,
    project_id       INT,
    replica_no       SMALLINT,
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
                namespace text NOT NULL,
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
    project_id       INT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    data_table_row     data_tables;
    distinct_table_row distinct_tables;
    time_point         BIGINT;
    namespace_point    TEXT;
BEGIN
    time_point := 1;

    EXECUTE format($$ SELECT "time", namespace FROM %s ORDER BY namespace LIMIT 1 $$,
                   copy_table_oid)
    INTO time_point, namespace_point;

    WHILE time_point IS NOT NULL LOOP
        SELECT *
        INTO distinct_table_row
        FROM get_distinct_table(project_id :: BIGINT, namespace_point, replica_no);

        SELECT *
        INTO data_table_row
        FROM get_data_table(time_point, project_id, namespace_point, replica_no,
                            partition_number, total_partitions);

        BEGIN
            EXECUTE format(
                $$
                    WITH selected AS
                    (
                        DELETE FROM %2$s
                        WHERE ("time" >= %3$L OR  %3$L IS NULL) and ("time" <= %4$L OR %4$L IS NULL)
                              AND namespace = %11$L
                        RETURNING *
                    ),
                    distinct_fields AS (
                        SELECT field_name as name
                        FROM data_fields
                        WHERE table_name = %1$L::regclass AND is_distinct = TRUE
                    ),
                    insert_distinct AS (
                        INSERT INTO  %6$s as former
                            SELECT distinct_fields.name, value->>distinct_fields.name, max(time)
                            FROM  distinct_fields, selected
                            WHERE value ? distinct_fields.name
                            GROUP BY distinct_fields.name, (value->>distinct_fields.name)
                            ON CONFLICT (field, value)
                                --DO NOTHING
                                DO UPDATE
                                SET last_time_approx = EXCLUDED.last_time_approx + (1e9::bigint * 60*60*24::bigint)
                                WHERE EXCLUDED.last_time_approx > former.last_time_approx
                                --DO UPDATE SET last_time_approx = EXCLUDED.last_time_approx
                    )
                    INSERT INTO %1$s (time, %12$s) SELECT time, %13$s FROM selected;
                $$, data_table_row.table_name, copy_table_oid, data_table_row.start_time,
                data_table_row.end_time, data_table_row, distinct_table_row.table_name,
                NULL, NULL, NULL,
                project_id, namespace_point,
                get_field_list(project_id, namespace_point),
                get_field_from_json_list(project_id, namespace_point))
            USING data_table_row;

            EXECUTE format($$ SELECT "time", namespace FROM %s  ORDER BY namespace LIMIT 1 $$, copy_table_oid)
            INTO time_point, namespace_point;
            EXCEPTION WHEN deadlock_detected THEN
            --do nothing, rerun loop (deadlock can be caused by concurrent updates to distinct table)
        END;
    END LOOP;
END
$BODY$;



CREATE OR REPLACE FUNCTION initialize_namespace(
    project_id       INT,
    namespace        TEXT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM create_cluster_table(project_id, namespace, replica_no);
    PERFORM create_master_table(project_id, namespace, replica_no, partition_number, total_partitions);
    PERFORM create_partition_table(project_id, namespace, replica_no, partition_number, total_partitions);
END
$BODY$;

CREATE OR REPLACE FUNCTION register_namespace(
    project_id       BIGINT,
    namespace        TEXT,
    replica_no       SMALLINT,
    partition_number SMALLINT,
    total_partitions SMALLINT
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM register_global_table(
        get_master_table_name(project_id, namespace, replica_no),
        get_cluster_table_name(project_id, namespace, replica_no)
    );
END
$BODY$;



