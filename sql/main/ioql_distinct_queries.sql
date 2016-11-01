CREATE OR REPLACE FUNCTION get_distinct_values_local(namespace_name NAME, field TEXT)
    RETURNS SETOF TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    table_name REGCLASS;
BEGIN
    table_name := get_distinct_local_table_oid(namespace_name);
    RETURN QUERY EXECUTE format(
        $$
            SELECT DISTINCT value
            FROM %s
            WHERE field = %L
        $$,
        table_name, field);
END
$BODY$;