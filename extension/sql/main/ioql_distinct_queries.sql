CREATE OR REPLACE FUNCTION get_distinct_values_local(hypertable_name NAME, replica_id SMALLINT, column_name TEXT)
    RETURNS SETOF TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    table_name REGCLASS;
BEGIN
    table_name := get_distinct_local_table_oid(hypertable_name, replica_id);
    RETURN QUERY EXECUTE format(
        $$
            SELECT DISTINCT value
            FROM %s
            WHERE column_name = %L
        $$,
        table_name, column_name);
END
$BODY$;
