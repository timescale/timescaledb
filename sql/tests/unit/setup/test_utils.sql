CREATE SCHEMA IF NOT EXISTS test_utils;

CREATE OR REPLACE FUNCTION test_utils.test_query(query ioql_query, expected_table_schema text, expected_table text)
RETURNS void
AS 
$$
DECLARE
cursor REFCURSOR;
expected_record record;
returned_record record;
BEGIN
    SELECT ioql_exec_query_record_cursor(query, 'cursor') into cursor;

    FOR expected_record in EXECUTE format('SELECT * FROM %I.%I', expected_table_schema, expected_table)
    LOOP    
        FETCH cursor INTO returned_record;
        IF FOUND = FALSE THEN 
            raise exception 'Expected row: %v got nothing', to_jsonb(expected_record);
            EXIT;
        END IF;

        -- Record comparison fails on different types of columns
        IF expected_record != returned_record THEN
            raise exception 'Expected row: %v got: %v', to_jsonb(expected_record), to_jsonb(returned_record);
            EXIT;
        END IF;
    END LOOP;

    CLOSE cursor;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_utils.query_to_table(query ioql_query, output_table text)
RETURNS boolean
AS 
$$
DECLARE
prepared_query text;
BEGIN
    SELECT ioql_exec_query_record_sql(query) into prepared_query;
    EXECUTE format('DROP TABLE IF EXISTS %I', output_table);
    EXECUTE format('CREATE TABLE %I AS %s WITH DATA', output_table, prepared_query); 
    RETURN TRUE;
END
$$
LANGUAGE plpgsql;