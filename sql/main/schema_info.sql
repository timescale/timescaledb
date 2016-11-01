CREATE OR REPLACE FUNCTION get_partitioning_field_name(
    namespace_name NAME
)
    RETURNS NAME LANGUAGE SQL STABLE AS
$BODY$
SELECT name
FROM field f
WHERE f.namespace_name = get_partitioning_field_name.namespace_name AND f.is_partitioning;
$BODY$;

CREATE OR REPLACE FUNCTION get_schema_name(
    namespace_name NAME
)
    RETURNS NAME LANGUAGE SQL STABLE AS
$BODY$
SELECT schema_name
FROM namespace AS n
WHERE n.name = namespace_name;
$BODY$;


CREATE OR REPLACE FUNCTION get_distinct_local_table_oid(
    namespace_name NAME
)
    RETURNS REGCLASS LANGUAGE SQL STABLE AS
$BODY$
SELECT format('%I.%I', n.schema_name, nn.distinct_local_table_name) :: REGCLASS
FROM namespace AS n
INNER JOIN namespace_node AS nn ON (n.name = nn.namespace_name)
WHERE n.name = get_distinct_local_table_oid.namespace_name AND
      nn.database_name = current_database();
$BODY$;

CREATE OR REPLACE FUNCTION get_field_names(
    namespace_name NAME
)
    RETURNS NAME [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
    SELECT name
    FROM field f
    WHERE f.namespace_name = get_field_names.namespace_name
    ORDER BY name
);
$BODY$;

CREATE OR REPLACE FUNCTION get_quoted_field_names(
    namespace_name NAME
)
    RETURNS TEXT [] LANGUAGE SQL STABLE AS
$BODY$
SELECT ARRAY(
    SELECT format('%I', name)
    FROM field f
    WHERE f.namespace_name = get_quoted_field_names.namespace_name
    ORDER BY name
);
$BODY$;

CREATE OR REPLACE FUNCTION get_field_names_and_types(
    namespace_name NAME,
    field_names    NAME []
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
                 WHERE f.namespace_name = get_field_names_and_types.namespace_name
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
                WHERE f.namespace_name = get_field_names_and_types.namespace_name AND
                      f.name = field_name
            );
            RAISE 'Missing field "%" in namespace "%"', missing_field, namespace_name
            USING ERRCODE = 'IO002';
        END;
    END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_field_type(
    namespace_name NAME,
    field_name     NAME
)
    RETURNS REGTYPE LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    data_type REGTYPE;
BEGIN
    SELECT f.data_type
    INTO data_type
    FROM field f
    WHERE f.name = get_field_type.field_name AND f.namespace_name = get_field_type.namespace_name;

    IF NOT FOUND THEN
        RAISE 'Missing field "%" in namespace "%"', field_name, namespace_name
        USING ERRCODE = 'IO002';
    END IF;
    RETURN data_type;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_cluster_table(
    namespace_name NAME
)
    RETURNS REGCLASS LANGUAGE SQL STABLE AS
$BODY$
SELECT format('%I.%I', schema_name, cluster_table_name) :: REGCLASS
FROM namespace AS n
WHERE n.name = namespace_name
$BODY$;