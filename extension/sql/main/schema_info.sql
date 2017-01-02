CREATE OR REPLACE FUNCTION get_distinct_table_oid(
    hypertable_name NAME,
    replica_id      SMALLINT,
    database_name   NAME
)
    RETURNS REGCLASS LANGUAGE SQL STABLE AS
$BODY$
SELECT format('%I.%I', drn.schema_name, drn.table_name) :: REGCLASS
FROM distinct_replica_node AS drn
WHERE drn.hypertable_name = get_distinct_table_oid.hypertable_name AND
      drn.replica_id = get_distinct_table_oid.replica_id AND
      drn.database_name = get_distinct_table_oid.database_name;
$BODY$;


CREATE OR REPLACE FUNCTION get_distinct_local_table_oid(
    hypertable_name NAME,
    replica_id      SMALLINT
)
    RETURNS REGCLASS LANGUAGE SQL STABLE AS
$BODY$
SELECT get_distinct_table_oid(hypertable_name, replica_id, current_database())
$BODY$;

CREATE OR REPLACE FUNCTION get_time_field(
    hypertable_name NAME
)
    RETURNS NAME LANGUAGE SQL STABLE AS
$BODY$
    SELECT time_field_name
    FROM hypertable h
    WHERE h.name = hypertable_name;
$BODY$;

CREATE OR REPLACE FUNCTION get_field_names(
    hypertable_name NAME
)
    RETURNS NAME [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
    SELECT name
    FROM field f
    WHERE f.hypertable_name = get_field_names.hypertable_name
    ORDER BY attnum
);
$BODY$;

CREATE OR REPLACE FUNCTION get_quoted_field_names(
    hypertable_name NAME
)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
    SELECT format('%I', name)
    FROM field f
    WHERE f.hypertable_name = get_quoted_field_names.hypertable_name
    ORDER BY name
);
$BODY$;

CREATE OR REPLACE FUNCTION get_field_names_and_types(
    hypertable_name NAME,
    field_names     NAME []
)
    RETURNS TABLE(field NAME, data_type REGTYPE) LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    rows_returned INT;
BEGIN
    RETURN QUERY SELECT
                     f.name,
                     f.data_type
                 FROM field f
                 INNER JOIN unnest(field_names) WITH ORDINALITY
                     AS x(field_name, ordering) ON f.name = x.field_name
                 WHERE f.hypertable_name = get_field_names_and_types.hypertable_name
                 ORDER BY x.ordering;
    GET DIAGNOSTICS rows_returned = ROW_COUNT;
    IF rows_returned != cardinality(field_names) THEN
        DECLARE
            missing_field NAME;
        BEGIN
            SELECT field_name
            INTO missing_field
            FROM unnest(field_names) AS field_name
            WHERE NOT EXISTS(
                SELECT 1
                FROM field f
                WHERE f.hypertable_name = get_field_names_and_types.hypertable_name AND
                      f.name = field_name
            );
            RAISE 'Missing field "%" in namespace "%"', missing_field, hypertable_name
            USING ERRCODE = 'IO002';
        END;
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_field_type(
    hypertable_name NAME,
    field_name      NAME
)
    RETURNS REGTYPE LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    data_type REGTYPE;
BEGIN
    SELECT f.data_type
    INTO data_type
    FROM field f
    WHERE f.name = get_field_type.field_name AND f.hypertable_name = get_field_type.hypertable_name;

    IF NOT FOUND THEN
        RAISE 'Missing field "%" in namespace "%"', field_name, hypertable_name
        USING ERRCODE = 'IO002';
    END IF;
    RETURN data_type;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_partition_for_epoch(
    epoch     partition_epoch,
    key_value TEXT
)
    RETURNS partition LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
DECLARE
    partition_row partition;
BEGIN
    EXECUTE format($$
    SELECT  p.*
    FROM  partition p 
    WHERE p.epoch_id = %L AND
    %s(%L, %L) BETWEEN p.keyspace_start AND p.keyspace_end 
  $$,
                   epoch.id, epoch.partitioning_func, key_value, epoch.partitioning_mod)
    INTO STRICT partition_row;

    RETURN partition_row;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_open_partition_for_key(
    hypertable_name NAME,
    key_value       TEXT
)
    RETURNS partition LANGUAGE SQL STABLE AS
$BODY$
SELECT p.*
FROM partition_epoch pe,
        get_partition_for_epoch(pe, key_value) p
WHERE pe.hypertable_name = get_open_partition_for_key.hypertable_name AND
      end_time IS NULL
$BODY$;




CREATE OR REPLACE FUNCTION is_main_table(
    table_oid regclass
)
    RETURNS bool LANGUAGE SQL STABLE AS
$BODY$
  SELECT EXISTS(SELECT 1 FROM hypertable WHERE main_table_name = relname AND main_schema_name = nspname)
  FROM pg_class c 
  INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
  WHERE c.OID = table_oid;
$BODY$;



CREATE OR REPLACE FUNCTION hypertable_from_main_table(
    table_oid regclass
)
    RETURNS hypertable LANGUAGE SQL STABLE AS
$BODY$
  SELECT h.*
  FROM pg_class c 
  INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
  INNER JOIN hypertable h ON (h.main_table_name = c.relname AND h.main_schema_name = n.nspname)
  WHERE c.OID = table_oid;
$BODY$;


CREATE OR REPLACE FUNCTION _sysinternal.time_col_name_for_crn(
    schema_name NAME,
    table_name  NAME
)
    RETURNS NAME LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    time_col_name NAME;
BEGIN
    SELECT h.time_field_name INTO STRICT time_col_name
    FROM hypertable h
    INNER JOIN partition_epoch pe ON (pe.hypertable_name = h.name)  
    INNER JOIN partition p ON (p.epoch_id = pe.id)
    INNER JOIN chunk c ON (c.partition_id = p.id)
    INNER JOIN chunk_replica_node crn ON (crn.chunk_id = c.id)
    WHERE crn.schema_name = time_col_name_for_crn.schema_name AND
    crn.table_name = time_col_name_for_crn.table_name;
    RETURN time_col_name;
END
$BODY$;

