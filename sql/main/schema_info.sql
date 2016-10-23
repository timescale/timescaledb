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
