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


CREATE OR REPLACE FUNCTION get_distinct_table_oid(
    namespace_name NAME
)
    RETURNS REGCLASS LANGUAGE SQL STABLE AS
$BODY$
SELECT format('%I.%I', n.schema_name, nn.distinct_local_table_name) :: REGCLASS
FROM namespace AS n
INNER JOIN namespace_node AS nn ON (n.name = nn.namespace_name)
WHERE n.name = get_distinct_table_oid.namespace_name AND
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