CREATE SCHEMA IF NOT EXISTS test_utils;

CREATE OR REPLACE FUNCTION test_utils.test_query(query ioql_query, expected_table_schema text, expected_table text)
RETURNS void
AS 
$BODY$
DECLARE
cursor REFCURSOR;
expected_record record;
returned_record record;
BEGIN
    --fix the ordering for equal time items
    OPEN cursor FOR EXECUTE format(
      $$
        SELECT *
        FROM (%s) as res
        ORDER BY time DESC NULLS LAST, res
      $$, ioql_exec_query_record_sql(query));

    --RAISE NOTICE 'testing %', expected_table;
    FOR expected_record in EXECUTE format('SELECT * FROM %I.%I as res ORDER BY time DESC NULLS LAST, res',
    expected_table_schema, expected_table)
    LOOP    
        FETCH cursor INTO returned_record;
        IF FOUND = FALSE THEN 
            raise exception 'Expected row: % got nothing', to_jsonb(expected_record);
            EXIT;
        END IF;

        -- Record comparison fails on different types of columns
        IF expected_record != returned_record THEN
            raise exception 'Expected row: % got: % (expected table %)', to_jsonb(expected_record), to_jsonb(returned_record), expected_table;
            EXIT;
        END IF;
    END LOOP;

    FETCH cursor INTO returned_record;
    IF FOUND = TRUE THEN
        RAISE EXCEPTION 'Unexpected row: %v', to_jsonb(returned_record);
    END IF;

    CLOSE cursor;
END
$BODY$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_utils.query_to_table(query ioql_query, output_table text)
RETURNS boolean
AS
$BODY$
DECLARE
prepared_query text;
BEGIN
    SELECT ioql_exec_query_record_sql(query) into prepared_query;
    EXECUTE format('DROP TABLE IF EXISTS %I', output_table);
    EXECUTE format('CREATE TABLE %I AS %s WITH DATA', output_table, prepared_query); 
    RETURN TRUE;
END
$BODY$
LANGUAGE plpgsql;
