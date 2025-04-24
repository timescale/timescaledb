-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
-- We have support for a dedicated type whos only purpose is to
-- generate error. You pass in the error code you want and it will be
-- raised from inside the input function.
CREATE TYPE sqlstate_raise;
CREATE FUNCTION ts_sqlstate_raise_in(cstring)
    RETURNS sqlstate_raise
    AS :MODULE_PATHNAME LANGUAGE C STABLE STRICT;
CREATE FUNCTION ts_sqlstate_raise_out(sqlstate_raise)
    RETURNS cstring
    AS :MODULE_PATHNAME LANGUAGE C STABLE STRICT;
CREATE TYPE sqlstate_raise (
    input = ts_sqlstate_raise_in,
    output = ts_sqlstate_raise_out
);

CREATE OR REPLACE FUNCTION test_with_clause_filter(with_clauses TEXT[][])
    RETURNS TABLE(namespace TEXT, name TEXT, value TEXT, filtered BOOLEAN)
    AS :MODULE_PATHNAME, 'ts_test_with_clause_filter' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION test_with_clause_parse(with_clauses TEXT[][])
    RETURNS TABLE(name TEXT, unimpl INT8, bool BOOLEAN, int32 INT4, def INT4, name_arg NAME, regc REGCLASS)
    AS :MODULE_PATHNAME, 'ts_test_with_clause_parse' LANGUAGE C VOLATILE STRICT;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER


SELECT * FROM test_with_clause_filter(
        '{
            {"baz", "bar", "foo"},
            {"timescaledb", "bar", "baz"},
            {"bar", "timescaledb", "baz"},
            {"timescaledb", "baz", "bar"},
            {"tsdb", "qux", "bar"},
            {"tsdb", "quux", "bar"}
        }');

SELECT * FROM test_with_clause_filter(
        '{
            {"baz", "bar", "foo"},
            {"bar", "timescaledb", "baz"},
            {"bar", "timescaledb", "baz"}
        }');

SELECT * FROM test_with_clause_filter(
        '{
            {"bar", "timescaledb"},
            {"baz", "bar"},
            {"timescaledb", "bar"},
            {"timescaledb", "baz"}
        }');

SELECT * FROM test_with_clause_filter(
        '{
            {"timescaledb"},
            {"bar"},
            {"baz"}
        }');

\set ON_ERROR_STOP 0
-- unrecognized argument
SELECT * FROM test_with_clause_parse('{{"timescaledb", "fakearg", "bar"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "fakearg"}}');

-- unimplemented handled gracefully
SELECT * FROM test_with_clause_parse('{{"timescaledb", "unimplemented", "bar"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "unimplemented", "true"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "unimplemented"}}');
\set ON_ERROR_STOP 1

-- bool parsing
SELECT * FROM test_with_clause_parse('{{"timescaledb", "bool", "true"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "bool", "false"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "bool", "on"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "bool", "off"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "bool", "1"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "bool", "0"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "bool"}}');

-- int32 parsing
SELECT * FROM test_with_clause_parse('{{"timescaledb", "int32", "1"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "int32", "572"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "int32", "-10"}}');
\set ON_ERROR_STOP 0
SELECT * FROM test_with_clause_parse('{{"timescaledb", "int32", "true"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "int32", "bar"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "int32"}}');
\set ON_ERROR_STOP 1


-- name parsing
SELECT * FROM test_with_clause_parse('{{"timescaledb", "name", "1"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "name", "572"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "name", "-10"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "name", "true"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "name", "bar"}}');
\set ON_ERROR_STOP 0
SELECT * FROM test_with_clause_parse('{{"timescaledb", "name"}}');
\set ON_ERROR_STOP 1

-- REGCLASS parsing
SELECT * FROM test_with_clause_parse('{{"timescaledb", "regclass", "pg_type"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "regclass", "1"}}');
\set ON_ERROR_STOP 0
SELECT * FROM test_with_clause_parse('{{"timescaledb", "regclass", "-10"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "regclass", "true"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "regclass", "bar"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "regclass"}}');
\set ON_ERROR_STOP 1

-- Test errors generated from inside the input function
\set ON_ERROR_STOP 0
-- Out of memory, "hard" error.
SELECT * FROM test_with_clause_parse('{{"timescaledb", "sqlstate_raise", "53200"}}');
-- Division by zero, "soft" error. Shows invalid value message (with a
-- strange message in this case).
SELECT * FROM test_with_clause_parse('{{"timescaledb", "sqlstate_raise", "22012"}}');
\set ON_ERROR_STOP 1

-- defaults get overridden
SELECT * FROM test_with_clause_parse('{{"timescaledb", "default", "1"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "default", "572"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "default", "-10"}}');
\set ON_ERROR_STOP 0
SELECT * FROM test_with_clause_parse('{{"timescaledb", "default", "true"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "default", "bar"}}');
SELECT * FROM test_with_clause_parse('{{"timescaledb", "default"}}');
\set ON_ERROR_STOP 1


\set ON_ERROR_STOP 0
-- duplicates error
SELECT * FROM test_with_clause_parse('{{"timescaledb", "name", "1"}, {"timescaledb", "name", "572"}}');
\set ON_ERROR_STOP 1

-- multiple args
SELECT * FROM test_with_clause_parse('{
    {"a", "bool", "true"},
    {"b", "int32", "572"},
    {"c", "name", "bar"},
    {"d", "regclass", "pg_type"}
}');

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP TYPE sqlstate_raise CASCADE;
