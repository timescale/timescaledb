/********************************************************************************
The PostgreSQL License

Copyright (c) 2014, Binod Nepal, Mix Open Foundation (http://mixof.org).

Permission to use, copy, modify, and distribute this software and its documentation 
for any purpose, without fee, and without a written agreement is hereby granted, 
provided that the above copyright notice and this paragraph and 
the following two paragraphs appear in all copies.

IN NO EVENT SHALL MIX OPEN FOUNDATION BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, 
SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, 
ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF 
MIX OPEN FOUNDATION HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

MIX OPEN FOUNDATION SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, 
BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS 
FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, 
AND MIX OPEN FOUNDATION HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, 
UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
***********************************************************************************/

CREATE SCHEMA IF NOT EXISTS assert;
CREATE SCHEMA IF NOT EXISTS unit_tests;

DO 
$$
BEGIN
    IF NOT EXISTS 
    (
        SELECT * FROM pg_type
        WHERE 
            typname ='test_result'
        AND 
            typnamespace = 
            (
                SELECT oid FROM pg_namespace 
                WHERE nspname ='public'
            )
    ) THEN
        CREATE DOMAIN public.test_result AS text;
    END IF;
END
$$
LANGUAGE plpgsql;


DROP TABLE IF EXISTS unit_tests.test_details CASCADE;
DROP TABLE IF EXISTS unit_tests.tests CASCADE;
DROP TABLE IF EXISTS unit_tests.dependencies CASCADE;
CREATE TABLE unit_tests.tests
(
    test_id                                 SERIAL NOT NULL PRIMARY KEY,
    started_on                              TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT(CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    completed_on                            TIMESTAMP WITHOUT TIME ZONE NULL,
    total_tests                             integer NULL DEFAULT(0),
    failed_tests                            integer NULL DEFAULT(0),
    skipped_tests                           integer NULL DEFAULT(0)
);

CREATE INDEX unit_tests_tests_started_on_inx
ON unit_tests.tests(started_on);

CREATE INDEX unit_tests_tests_completed_on_inx
ON unit_tests.tests(completed_on);

CREATE INDEX unit_tests_tests_failed_tests_inx
ON unit_tests.tests(failed_tests);

CREATE TABLE unit_tests.test_details
(
    id                                      BIGSERIAL NOT NULL PRIMARY KEY,
    test_id                                 integer NOT NULL REFERENCES unit_tests.tests(test_id),
    function_name                           text NOT NULL,
    message                                 text NOT NULL,
    ts                                      TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT(CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    status                                  boolean NOT NULL,
    executed                                boolean NOT NULL
);

CREATE INDEX unit_tests_test_details_test_id_inx
ON unit_tests.test_details(test_id);

CREATE INDEX unit_tests_test_details_status_inx
ON unit_tests.test_details(status);

CREATE TABLE unit_tests.dependencies
(
    dependency_id                           BIGSERIAL NOT NULL PRIMARY KEY,
    dependent_ns                            text,
    dependent_function_name                 text NOT NULL,
    depends_on_ns                           text,
    depends_on_function_name                text NOT NULL
);

CREATE INDEX unit_tests_dependencies_dependency_id_inx
ON unit_tests.dependencies(dependency_id);


DROP FUNCTION IF EXISTS assert.fail(message text);
CREATE FUNCTION assert.fail(message text)
RETURNS text
AS
$$
BEGIN
    IF $1 IS NULL OR trim($1) = '' THEN
        message := 'NO REASON SPECIFIED';
    END IF;
    
    RAISE WARNING 'ASSERT FAILED : %', message;
    RETURN message;
END
$$
LANGUAGE plpgsql
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS assert.pass(message text);
CREATE FUNCTION assert.pass(message text)
RETURNS text
AS
$$
BEGIN
    RAISE NOTICE 'ASSERT PASSED : %', message;
    RETURN '';
END
$$
LANGUAGE plpgsql
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS assert.ok(message text);
CREATE FUNCTION assert.ok(message text)
RETURNS text
AS
$$
BEGIN
    RAISE NOTICE 'OK : %', message;
    RETURN '';
END
$$
LANGUAGE plpgsql
IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS assert.is_equal(IN have anyelement, IN want anyelement, OUT message text, OUT result boolean);
CREATE FUNCTION assert.is_equal(IN have anyelement, IN want anyelement, OUT message text, OUT result boolean)
AS
$$
BEGIN
    IF($1 IS NOT DISTINCT FROM $2) THEN
        message := 'Assert is equal.';
        PERFORM assert.ok(message);
        result := true;
        RETURN;
    END IF;
    
    message := E'ASSERT IS_EQUAL FAILED.\n\nHave -> ' || COALESCE($1::text, 'NULL') || E'\nWant -> ' || COALESCE($2::text, 'NULL') || E'\n';    
    PERFORM assert.fail(message);
    result := false;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;


DROP FUNCTION IF EXISTS assert.are_equal(VARIADIC anyarray, OUT message text, OUT result boolean);
CREATE FUNCTION assert.are_equal(VARIADIC anyarray, OUT message text, OUT result boolean)
AS
$$
    DECLARE count integer=0;
    DECLARE total_items bigint;
    DECLARE total_rows bigint;
BEGIN
    result := false;
    
    WITH counter
    AS
    (
        SELECT *
        FROM explode_array($1) AS items
    )
    SELECT
        COUNT(items),
        COUNT(*)
    INTO
        total_items,
        total_rows
    FROM counter;

    IF(total_items = 0 OR total_items = total_rows) THEN
        result := true;
    END IF;

    IF(result AND total_items > 0) THEN
        SELECT COUNT(DISTINCT $1[s.i])
        INTO count
        FROM generate_series(array_lower($1,1), array_upper($1,1)) AS s(i)
        ORDER BY 1;

        IF count <> 1 THEN
            result := FALSE;
        END IF;
    END IF;

    IF(NOT result) THEN
        message := 'ASSERT ARE_EQUAL FAILED.';  
        PERFORM assert.fail(message);
        RETURN;
    END IF;

    message := 'Asserts are equal.';
    PERFORM assert.ok(message);
    result := true;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;

DROP FUNCTION IF EXISTS assert.is_not_equal(IN already_have anyelement, IN dont_want anyelement, OUT message text, OUT result boolean);
CREATE FUNCTION assert.is_not_equal(IN already_have anyelement, IN dont_want anyelement, OUT message text, OUT result boolean)
AS
$$
BEGIN
    IF($1 IS DISTINCT FROM $2) THEN
        message := 'Assert is not equal.';
        PERFORM assert.ok(message);
        result := true;
        RETURN;
    END IF;
    
    message := E'ASSERT IS_NOT_EQUAL FAILED.\n\nAlready Have -> ' || COALESCE($1::text, 'NULL') || E'\nDon''t Want   -> ' || COALESCE($2::text, 'NULL') || E'\n';   
    PERFORM assert.fail(message);
    result := false;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;

DROP FUNCTION IF EXISTS assert.are_not_equal(VARIADIC anyarray, OUT message text, OUT result boolean);
CREATE FUNCTION assert.are_not_equal(VARIADIC anyarray, OUT message text, OUT result boolean)
AS
$$
    DECLARE count integer=0;
    DECLARE count_nulls bigint;
BEGIN    
    SELECT COUNT(*)
    INTO count_nulls
    FROM explode_array($1) AS items
    WHERE items IS NULL;

    SELECT COUNT(DISTINCT $1[s.i]) INTO count
    FROM generate_series(array_lower($1,1), array_upper($1,1)) AS s(i)
    ORDER BY 1;
    
    IF(count + count_nulls <> array_upper($1,1) OR count_nulls > 1) THEN
        message := 'ASSERT ARE_NOT_EQUAL FAILED.';  
        PERFORM assert.fail(message);
        RESULT := FALSE;
        RETURN;
    END IF;

    message := 'Asserts are not equal.';
    PERFORM assert.ok(message);
    result := true;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;


DROP FUNCTION IF EXISTS assert.is_null(IN anyelement, OUT message text, OUT result boolean);
CREATE FUNCTION assert.is_null(IN anyelement, OUT message text, OUT result boolean)
AS
$$
BEGIN
    IF($1 IS NULL) THEN
        message := 'Assert is NULL.';
        PERFORM assert.ok(message);
        result := true;
        RETURN;
    END IF;
    
    message := E'ASSERT IS_NULL FAILED. NULL value was expected.\n\n\n';    
    PERFORM assert.fail(message);
    result := false;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;

DROP FUNCTION IF EXISTS assert.is_not_null(IN anyelement, OUT message text, OUT result boolean);
CREATE FUNCTION assert.is_not_null(IN anyelement, OUT message text, OUT result boolean)
AS
$$
BEGIN
    IF($1 IS NOT NULL) THEN
        message := 'Assert is not NULL.';
        PERFORM assert.ok(message);
        result := true;
        RETURN;
    END IF;
    
    message := E'ASSERT IS_NOT_NULL FAILED. The value is NULL.\n\n\n';  
    PERFORM assert.fail(message);
    result := false;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;

DROP FUNCTION IF EXISTS assert.is_true(IN boolean, OUT message text, OUT result boolean);
CREATE FUNCTION assert.is_true(IN boolean, OUT message text, OUT result boolean)
AS
$$
BEGIN
    IF($1) THEN
        message := 'Assert is true.';
        PERFORM assert.ok(message);
        result := true;
        RETURN;
    END IF;
    
    message := E'ASSERT IS_TRUE FAILED. A true condition was expected.\n\n\n';  
    PERFORM assert.fail(message);
    result := false;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;

DROP FUNCTION IF EXISTS assert.is_false(IN boolean, OUT message text, OUT result boolean);
CREATE FUNCTION assert.is_false(IN boolean, OUT message text, OUT result boolean)
AS
$$
BEGIN
    IF(NOT $1) THEN
        message := 'Assert is false.';
        PERFORM assert.ok(message);
        result := true;
        RETURN;
    END IF;
    
    message := E'ASSERT IS_FALSE FAILED. A false condition was expected.\n\n\n';    
    PERFORM assert.fail(message);
    result := false;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;

DROP FUNCTION IF EXISTS assert.is_greater_than(IN x anyelement, IN y anyelement, OUT message text, OUT result boolean);
CREATE FUNCTION assert.is_greater_than(IN x anyelement, IN y anyelement, OUT message text, OUT result boolean)
AS
$$
BEGIN
    IF($1 > $2) THEN
        message := 'Assert greater than condition is satisfied.';
        PERFORM assert.ok(message);
        result := true;
        RETURN;
    END IF;
    
    message := E'ASSERT IS_GREATER_THAN FAILED.\n\n X : -> ' || COALESCE($1::text, 'NULL') || E'\n is not greater than Y:   -> ' || COALESCE($2::text, 'NULL') || E'\n';    
    PERFORM assert.fail(message);
    result := false;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;

DROP FUNCTION IF EXISTS assert.is_less_than(IN x anyelement, IN y anyelement, OUT message text, OUT result boolean);
CREATE FUNCTION assert.is_less_than(IN x anyelement, IN y anyelement, OUT message text, OUT result boolean)
AS
$$
BEGIN
    IF($1 < $2) THEN
        message := 'Assert less than condition is satisfied.';
        PERFORM assert.ok(message);
        result := true;
        RETURN;
    END IF;
    
    message := E'ASSERT IS_LESS_THAN FAILED.\n\n X : -> ' || COALESCE($1::text, 'NULL') || E'\n is not less than Y:   -> ' || COALESCE($2::text, 'NULL') || E'\n';  
    PERFORM assert.fail(message);
    result := false;
    RETURN;
END
$$
LANGUAGE plpgsql
IMMUTABLE;

DROP FUNCTION IF EXISTS assert.function_exists(function_name text, OUT message text, OUT result boolean);
CREATE FUNCTION assert.function_exists(function_name text, OUT message text, OUT result boolean)
AS
$$
BEGIN
    IF NOT EXISTS
    (
        SELECT  1
        FROM    pg_catalog.pg_namespace n
        JOIN    pg_catalog.pg_proc p
        ON      pronamespace = n.oid
        WHERE replace(nspname || '.' || proname || '(' || oidvectortypes(proargtypes) || ')', ' ' , '')::text=$1
    ) THEN
        message := format('The function %s does not exist.', $1);
        PERFORM assert.fail(message);

        result := false;
        RETURN;
    END IF;

    message := format('Ok. The function %s exists.', $1);
    PERFORM assert.ok(message);
    result := true;
    RETURN;
END
$$
LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS assert.if_functions_compile(VARIADIC _schema_name text[], OUT message text, OUT result boolean);
CREATE OR REPLACE FUNCTION assert.if_functions_compile
(
    VARIADIC _schema_name text[],
    OUT message text, 
    OUT result boolean
)
AS
$$
    DECLARE all_parameters              text;
    DECLARE current_function            RECORD;
    DECLARE current_function_name       text;
    DECLARE current_type                text;
    DECLARE current_type_schema         text;
    DECLARE current_parameter           text;
    DECLARE functions_count             smallint := 0;
    DECLARE current_parameters_count    int;
    DECLARE i                           int;
    DECLARE command_text                text;
    DECLARE failed_functions            text;
BEGIN
    FOR current_function IN 
        SELECT proname, proargtypes, nspname 
        FROM pg_proc 
        INNER JOIN pg_namespace 
        ON pg_proc.pronamespace = pg_namespace.oid 
        WHERE pronamespace IN 
        (
            SELECT oid FROM pg_namespace 
            WHERE nspname = ANY($1) 
            AND nspname NOT IN
            (
                'assert', 'unit_tests', 'information_schema'
            ) 
            AND proname NOT IN('if_functions_compile')
        ) 
    LOOP
        current_parameters_count := array_upper(current_function.proargtypes, 1) + 1;

        i := 0;
        all_parameters := '';

        LOOP
        IF i < current_parameters_count THEN
            IF i > 0 THEN
                all_parameters := all_parameters || ', ';
            END IF;

            SELECT 
                nspname, typname 
            INTO 
                current_type_schema, current_type 
            FROM pg_type 
            INNER JOIN pg_namespace 
            ON pg_type.typnamespace = pg_namespace.oid
            WHERE pg_type.oid = current_function.proargtypes[i];

            IF(current_type IN('int4', 'int8', 'numeric', 'integer_strict', 'money_strict','decimal_strict', 'integer_strict2', 'money_strict2','decimal_strict2', 'money','decimal', 'numeric', 'bigint')) THEN
                current_parameter := '1::' || current_type_schema || '.' || current_type;
            ELSIF(substring(current_type, 1, 1) = '_') THEN
                current_parameter := 'NULL::' || current_type_schema || '.' || substring(current_type, 2, length(current_type)) || '[]';
            ELSIF(current_type in ('date')) THEN            
                current_parameter := '''1-1-2000''::' || current_type;
            ELSIF(current_type = 'bool') THEN
                current_parameter := 'false';            
            ELSE
                current_parameter := '''''::' || quote_ident(current_type_schema) || '.' || quote_ident(current_type);
            END IF;
            
            all_parameters = all_parameters || current_parameter;

            i := i + 1;
        ELSE
            EXIT;
        END IF;
    END LOOP;

    BEGIN
        current_function_name := quote_ident(current_function.nspname)  || '.' || quote_ident(current_function.proname);
        command_text := 'SELECT * FROM ' || current_function_name || '(' || all_parameters || ');';

        EXECUTE command_text;
        functions_count := functions_count + 1;

        EXCEPTION WHEN OTHERS THEN
            IF(failed_functions IS NULL) THEN 
                failed_functions := '';
            END IF;
            
            IF(SQLSTATE IN('42702', '42704')) THEN
                failed_functions := failed_functions || E'\n' || command_text || E'\n' || SQLERRM || E'\n';                
            END IF;
    END;


    END LOOP;

    IF(failed_functions != '') THEN
        message := E'The test if_functions_compile failed. The following functions failed to compile : \n\n' || failed_functions;
        result := false;
        PERFORM assert.fail(message);
        RETURN;
    END IF;
END;
$$
LANGUAGE plpgsql 
VOLATILE;

DROP FUNCTION IF EXISTS assert.if_views_compile(VARIADIC _schema_name text[], OUT message text, OUT result boolean);
CREATE FUNCTION assert.if_views_compile
(
    VARIADIC _schema_name text[],
    OUT message text, 
    OUT result boolean    
)
AS
$$

    DECLARE message                     test_result;
    DECLARE current_view                RECORD;
    DECLARE current_view_name           text;
    DECLARE command_text                text;
    DECLARE failed_views                text;
BEGIN
    FOR current_view IN 
        SELECT table_name, table_schema 
        FROM information_schema.views
        WHERE table_schema = ANY($1) 
    LOOP

    BEGIN
        current_view_name := quote_ident(current_view.table_schema)  || '.' || quote_ident(current_view.table_name);
        command_text := 'SELECT * FROM ' || current_view_name || ' LIMIT 1;';

        RAISE NOTICE '%', command_text;
        
        EXECUTE command_text;

        EXCEPTION WHEN OTHERS THEN
            IF(failed_views IS NULL) THEN 
                failed_views := '';
            END IF;

            failed_views := failed_views || E'\n' || command_text || E'\n' || SQLERRM || E'\n';                
    END;


    END LOOP;

    IF(failed_views != '') THEN
        message := E'The test if_views_compile failed. The following views failed to compile : \n\n' || failed_views;
        result := false;
        PERFORM assert.fail(message);
        RETURN;
    END IF;

    RETURN;
END;
$$
LANGUAGE plpgsql 
VOLATILE;


DROP FUNCTION IF EXISTS unit_tests.add_dependency(p_dependent text, p_depends_on text);
CREATE FUNCTION unit_tests.add_dependency(p_dependent text, p_depends_on text)
  RETURNS void
AS
  $$
    DECLARE dependent_ns text;
    DECLARE dependent_name text;
    DECLARE depends_on_ns text;
    DECLARE depends_on_name text;
    DECLARE arr text[];
BEGIN
    IF p_dependent LIKE '%.%' THEN
        SELECT regexp_split_to_array(p_dependent, E'\\.') INTO arr;
        SELECT arr[1] INTO dependent_ns;
        SELECT arr[2] INTO dependent_name;
    ELSE
        SELECT NULL INTO dependent_ns;
        SELECT p_dependent INTO dependent_name;
    END IF;
    IF p_depends_on LIKE '%.%' THEN
        SELECT regexp_split_to_array(p_depends_on, E'\\.') INTO arr;
        SELECT arr[1] INTO depends_on_ns;
        SELECT arr[2] INTO depends_on_name;
    ELSE
        SELECT NULL INTO depends_on_ns;
        SELECT p_depends_on INTO depends_on_name;
    END IF;
    INSERT INTO unit_tests.dependencies (dependent_ns, dependent_function_name, depends_on_ns, depends_on_function_name)
    VALUES (dependent_ns, dependent_name, depends_on_ns, depends_on_name);
END
$$
LANGUAGE plpgsql
STRICT;


DROP FUNCTION IF EXISTS unit_tests.begin(verbosity integer, format text);
CREATE FUNCTION unit_tests.begin(verbosity integer DEFAULT 9, format text DEFAULT '')
RETURNS TABLE(message text, result character(1))
AS
$$
    DECLARE this                    record;
    DECLARE _function_name          text;
    DECLARE _sql                    text;
    DECLARE _failed_dependencies    text[];
    DECLARE _num_of_test_functions  integer;
    DECLARE _should_skip            boolean;
    DECLARE _message                text;
    DECLARE _error                  text;
    DECLARE _context                text;
    DECLARE _result                 character(1);
    DECLARE _test_id                integer;
    DECLARE _status                 boolean;
    DECLARE _total_tests            integer                         = 0;
    DECLARE _failed_tests           integer                         = 0;
    DECLARE _skipped_tests          integer                         = 0;
    DECLARE _list_of_failed_tests   text;
    DECLARE _list_of_skipped_tests  text;
    DECLARE _started_from           TIMESTAMP WITHOUT TIME ZONE;
    DECLARE _completed_on           TIMESTAMP WITHOUT TIME ZONE;
    DECLARE _delta                  integer;
    DECLARE _ret_val                text                            = '';
    DECLARE _verbosity              text[]                          =
                                    ARRAY['debug5', 'debug4', 'debug3', 'debug2', 'debug1', 'log', 'notice', 'warning', 'error', 'fatal', 'panic'];
BEGIN
    _started_from := clock_timestamp() AT TIME ZONE 'UTC';

    IF(format='teamcity') THEN
        RAISE INFO '##teamcity[testSuiteStarted name=''Plpgunit'' message=''Test started from : %'']', _started_from;
    ELSE
        RAISE INFO 'Test started from : %', _started_from;
    END IF;

    IF($1 > 11) THEN
        $1 := 9;
    END IF;

    EXECUTE 'SET CLIENT_MIN_MESSAGES TO ' || _verbosity[$1];
    RAISE WARNING 'CLIENT_MIN_MESSAGES set to : %' , _verbosity[$1];

    SELECT nextval('unit_tests.tests_test_id_seq') INTO _test_id;

    INSERT INTO unit_tests.tests(test_id)
    SELECT _test_id;

    DROP TABLE IF EXISTS temp_test_functions;
    CREATE TEMP TABLE temp_test_functions AS
    SELECT
        nspname AS ns_name,
        proname AS function_name,
        p.oid as oid
    FROM    pg_catalog.pg_namespace n
    JOIN    pg_catalog.pg_proc p
    ON      pronamespace = n.oid
    WHERE
        prorettype='test_result'::regtype::oid;

    SELECT count(*) INTO _num_of_test_functions FROM temp_test_functions;

    DROP TABLE IF EXISTS temp_dependency_levels;
    CREATE TEMP TABLE temp_dependency_levels AS
    WITH RECURSIVE dependency_levels(ns_name, function_name, oid, level) AS (
      -- select functions without any dependencies
      SELECT ns_name, function_name, tf.oid, 0 as level
      FROM temp_test_functions tf
      LEFT OUTER JOIN unit_tests.dependencies d ON tf.ns_name = d.dependent_ns AND tf.function_name = d.dependent_function_name
      WHERE d.dependency_id IS NULL
      UNION
      -- add functions which depend on the previous level functions
      SELECT d.dependent_ns, d.dependent_function_name, tf.oid, level + 1
      FROM dependency_levels dl
      JOIN unit_tests.dependencies d ON dl.ns_name = d.depends_on_ns AND dl.function_name LIKE d.depends_on_function_name
      JOIN temp_test_functions tf ON d.dependent_ns = tf.ns_name AND d.dependent_function_name = tf.function_name
      WHERE level < _num_of_test_functions -- don't follow circles for too long
      )
      SELECT ns_name, function_name, oid, max(level) as max_level
      FROM dependency_levels
      GROUP BY ns_name, function_name, oid;

    IF (SELECT count(*) < _num_of_test_functions FROM temp_dependency_levels) THEN
      SELECT array_to_string(array_agg(tf.ns_name || '.' || tf.function_name || '()'), ', ')
      INTO _error
      FROM temp_test_functions tf
      LEFT OUTER JOIN temp_dependency_levels dl ON tf.oid = dl.oid
        WHERE dl.oid IS NULL;
      RAISE EXCEPTION 'Cyclic dependencies detected. Check the following test functions: %', _error;
    END IF;

    IF exists(SELECT * FROM temp_dependency_levels WHERE max_level = _num_of_test_functions) THEN
      SELECT array_to_string(array_agg(ns_name || '.' || function_name || '()'), ', ')
        INTO _error
        FROM temp_dependency_levels
        WHERE max_level = _num_of_test_functions;
      RAISE EXCEPTION 'Cyclic dependencies detected. Check the dependency graph including following test functions: %', _error;
    END IF;

    FOR this IN
      SELECT ns_name, function_name, max_level
        FROM temp_dependency_levels
        ORDER BY max_level, oid
    LOOP
        BEGIN
            _status := false;
            _total_tests := _total_tests + 1;

            _function_name = this.ns_name|| '.' || this.function_name || '()';

            SELECT array_agg(td.function_name)
                INTO _failed_dependencies
                FROM unit_tests.dependencies d
                JOIN unit_tests.test_details td on td.function_name LIKE d.depends_on_ns || '.' || d.depends_on_function_name || '()'
                WHERE d.dependent_ns = this.ns_name AND d.dependent_function_name = this.function_name
                AND test_id = _test_id AND status = false;

            SELECT _failed_dependencies IS NOT NULL INTO _should_skip;
            IF NOT _should_skip THEN
                _sql := 'SELECT ' || _function_name || ';';

                RAISE NOTICE 'RUNNING TEST : %.', _function_name;

                IF(format='teamcity') THEN
                    RAISE INFO '##teamcity[testStarted name=''%'' message=''%'']', _function_name, _started_from;
                ELSE
                    RAISE INFO 'Running test % : %', _function_name, _started_from;
                END IF;

                EXECUTE _sql INTO _message;

                IF _message = '' THEN
                    _status := true;

                    IF(format='teamcity') THEN
                        RAISE INFO '##teamcity[testFinished name=''%'' message=''%'']', _function_name, clock_timestamp() AT TIME ZONE 'UTC';
                    ELSE
                        RAISE INFO 'Passed % : %', _function_name, clock_timestamp() AT TIME ZONE 'UTC';
                    END IF;
                ELSE
                    IF(format='teamcity') THEN
                        RAISE INFO '##teamcity[testFailed name=''%'' message=''%'']', _function_name, _message;
                        RAISE INFO '##teamcity[testFinished name=''%'' message=''%'']', _function_name, clock_timestamp() AT TIME ZONE 'UTC';
                    ELSE
                        RAISE INFO 'Test failed % : %', _function_name, _message;
                    END IF;
                END IF;
            ELSE
                -- skipped test
                _status := true;
                _message = 'Failed dependencies: ' || array_to_string(_failed_dependencies, ',');
                IF(format='teamcity') THEN
                    RAISE INFO '##teamcity[testSkipped name=''%''] : %', _function_name, clock_timestamp() AT TIME ZONE 'UTC';
                ELSE
                    RAISE INFO 'Skipped % : %', _function_name, clock_timestamp() AT TIME ZONE 'UTC';
                END IF;
            END IF;

            INSERT INTO unit_tests.test_details(test_id, function_name, message, status, executed, ts)
            SELECT _test_id, _function_name, _message, _status, NOT _should_skip, clock_timestamp();

            IF NOT _status THEN
                _failed_tests := _failed_tests + 1;
                RAISE WARNING 'TEST % FAILED.', _function_name;
                RAISE WARNING 'REASON: %', _message;
            ELSIF NOT _should_skip THEN
                RAISE NOTICE 'TEST % COMPLETED WITHOUT ERRORS.', _function_name;
            ELSE
                _skipped_tests := _skipped_tests + 1;
                RAISE WARNING 'TEST % SKIPPED, BECAUSE A DEPENDENCY FAILED.', _function_name;
            END IF;

        EXCEPTION WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS _context = PG_EXCEPTION_CONTEXT;
            _message := 'ERR: [' || SQLSTATE || ']: ' || SQLERRM || E'\n    ' || split_part(_context, E'\n', 1);
            INSERT INTO unit_tests.test_details(test_id, function_name, message, status, executed)
            SELECT _test_id, _function_name, _message, false, true;

            _failed_tests := _failed_tests + 1;

            RAISE WARNING 'TEST % FAILED.', _function_name;
            RAISE WARNING 'REASON: %', _message;

            IF(format='teamcity') THEN
                RAISE INFO '##teamcity[testFailed name=''%'' message=''%'']', _function_name, _message;
                RAISE INFO '##teamcity[testFinished name=''%'' message=''%'']', _function_name, clock_timestamp() AT TIME ZONE 'UTC';
            ELSE
                RAISE INFO 'Test failed % : %', _function_name, _message;
            END IF;
        END;
    END LOOP;

    _completed_on := clock_timestamp() AT TIME ZONE 'UTC';
    _delta := extract(millisecond from _completed_on - _started_from)::integer;

    UPDATE unit_tests.tests
    SET total_tests = _total_tests, failed_tests = _failed_tests, skipped_tests = _skipped_tests, completed_on = _completed_on
    WHERE test_id = _test_id;

    IF format='junit' THEN
        SELECT
            '<?xml version="1.0" encoding="UTF-8"?>'||
            xmlelement
            (
                name testsuites,
                xmlelement
                (
                    name                    testsuite,
                    xmlattributes
                    (
                        'plpgunit'          AS name,
                        t.total_tests       AS tests,
                        t.failed_tests      AS failures,
                        0                   AS errors,
                        EXTRACT
                        (
                            EPOCH FROM t.completed_on - t.started_on
                        )                   AS time
                    ),
                    xmlagg
                    (
                        xmlelement
                        (
                            name testcase,
                            xmlattributes
                            (
                                td.function_name
                                            AS name,
                                EXTRACT
                                (
                                    EPOCH FROM td.ts - t.started_on
                                )           AS time
                            ),
                            CASE
                                WHEN td.status=false
                                THEN
                                    xmlelement
                                    (
                                        name failure,
                                        td.message
                                    )
                                END
                        )
                    )
                )
            ) INTO _ret_val
        FROM unit_tests.test_details td, unit_tests.tests t
        WHERE
            t.test_id=_test_id
        AND
            td.test_id=t.test_id
        GROUP BY t.test_id;
    ELSE
        WITH failed_tests AS
        (
            SELECT row_number() OVER (ORDER BY id) AS id,
                unit_tests.test_details.function_name,
                unit_tests.test_details.message
            FROM unit_tests.test_details
            WHERE test_id = _test_id
            AND status= false
        )
        SELECT array_to_string(array_agg(f.id::text || '. ' || f.function_name || ' --> ' || f.message), E'\n') INTO _list_of_failed_tests
        FROM failed_tests f;

        WITH skipped_tests AS
        (
            SELECT row_number() OVER (ORDER BY id) AS id,
                unit_tests.test_details.function_name,
                unit_tests.test_details.message
            FROM unit_tests.test_details
            WHERE test_id = _test_id
            AND executed = false
        )
        SELECT array_to_string(array_agg(s.id::text || '. ' || s.function_name || ' --> ' || s.message), E'\n') INTO _list_of_skipped_tests
        FROM skipped_tests s;

        _ret_val := _ret_val ||  'Test completed on : ' || _completed_on::text || E' UTC. \nTotal test runtime: ' || _delta::text || E' ms.\n';
        _ret_val := _ret_val || E'\nTotal tests run : ' || COALESCE(_total_tests, '0')::text;
        _ret_val := _ret_val || E'.\nPassed tests    : ' || (COALESCE(_total_tests, '0') - COALESCE(_failed_tests, '0') - COALESCE(_skipped_tests, '0'))::text;
        _ret_val := _ret_val || E'.\nFailed tests    : ' || COALESCE(_failed_tests, '0')::text;
        _ret_val := _ret_val || E'.\nSkipped tests   : ' || COALESCE(_skipped_tests, '0')::text;
        _ret_val := _ret_val || E'.\n\nList of failed tests:\n' || '----------------------';
        _ret_val := _ret_val || E'\n' || COALESCE(_list_of_failed_tests, '<NULL>')::text;
        _ret_val := _ret_val || E'.\n\nList of skipped tests:\n' || '----------------------';
        _ret_val := _ret_val || E'\n' || COALESCE(_list_of_skipped_tests, '<NULL>')::text;
        _ret_val := _ret_val || E'\n' || E'End of plpgunit test.\n\n';
    END IF;

    IF _failed_tests > 0 THEN
        _result := 'N';

        IF(format='teamcity') THEN
            RAISE INFO '##teamcity[testStarted name=''Result'']';
            RAISE INFO '##teamcity[testFailed name=''Result'' message=''%'']', REPLACE(_ret_val, E'\n', ' |n');
            RAISE INFO '##teamcity[testFinished name=''Result'']';
            RAISE INFO '##teamcity[testSuiteFinished name=''Plpgunit'' message=''%'']', REPLACE(_ret_val, E'\n', '|n');
        ELSE
            RAISE INFO '%', _ret_val;
        END IF;
    ELSE
        _result := 'Y';

        IF(format='teamcity') THEN
            RAISE INFO '##teamcity[testSuiteFinished name=''Plpgunit'' message=''%'']', REPLACE(_ret_val, E'\n', '|n');
        ELSE
            RAISE INFO '%', _ret_val;
        END IF;
    END IF;

    SET CLIENT_MIN_MESSAGES TO notice;

    RETURN QUERY SELECT _ret_val, _result;
END
$$
LANGUAGE plpgsql;

DROP FUNCTION IF EXISTS unit_tests.begin_junit(verbosity integer);
CREATE FUNCTION unit_tests.begin_junit(verbosity integer DEFAULT 9)
RETURNS TABLE(message text, result character(1))
AS
$$
BEGIN
    RETURN QUERY 
    SELECT * FROM unit_tests.begin($1, 'junit');
END
$$
LANGUAGE plpgsql;

-- version of begin that will raise if any tests have failed
-- this will cause psql to return nonzeo exit code so the build/script can be halted
CREATE OR REPLACE FUNCTION unit_tests.begin_psql(verbosity integer default 9, format text default '')
RETURNS VOID AS $$
    DECLARE
        _msg text;
        _res character(1);
    BEGIN
        SELECT * INTO _msg, _res
            FROM unit_tests.begin(verbosity, format)
        ;
        IF(_res != 'Y') THEN
            RAISE EXCEPTION 'Tests failed [%]', _msg;
        END IF;
    END
$$
LANGUAGE plpgsql;

