CREATE OR REPLACE FUNCTION get_distinct_values_local(hypertable_name NAME, replica_id SMALLINT, field TEXT)
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
            WHERE field = %L
        $$,
        table_name, field);
END
$BODY$;
