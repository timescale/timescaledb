-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- deparse each table%, drop it and recreate it using deparse result

DO
$$DECLARE
    tables CURSOR FOR
        SELECT tablename, schemaname
        FROM pg_tables
        WHERE tablename LIKE 'table%' AND (schemaname = 'public' OR schemaname = 'myschema')
        ORDER BY tablename;
    deparse_stmt text;
    tablename text;
BEGIN
    FOR table_record IN tables
    LOOP
        tablename := format('%I.%I', table_record.schemaname, table_record.tablename);
        EXECUTE format('SELECT _timescaledb_internal.get_tabledef(%L)', tablename) INTO deparse_stmt;
        EXECUTE 'DROP TABLE ' || tablename;
        EXECUTE deparse_stmt;
    END LOOP;
END$$;
