-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- We compare information(\d+) about manually created tables with the ones that were recreated using deparse_table command.
-- There should be no diff.

\set ECHO errors
\ir include/deparse_func.sql

SELECT format('%s/results/deparse_create.out', :'TEST_OUTPUT_DIR') AS "CREATE_OUT",
       format('%s/results/deparse_recreate.out', :'TEST_OUTPUT_DIR') AS "RECREATE_OUT"
\gset
SELECT format('\! diff %s %s', :'CREATE_OUT', :'RECREATE_OUT') as "DIFF_CMD"
\gset

\ir include/deparse_create.sql

\o :CREATE_OUT
\d+ "public".*

\ir include/deparse_recreate.sql
\o :RECREATE_OUT
\d+ "public".*

\o

:DIFF_CMD

SELECT 'TABLE DEPARSE TEST DONE';

\set ECHO all
-- test drop_chunks function deparsing
SELECT * FROM tsl_test_deparse_drop_chunks('myschema.table10', '2019-01-01'::timestamptz, verbose => true);
SELECT * FROM tsl_test_deparse_drop_chunks('table1', newer_than => 12345);
SELECT * FROM tsl_test_deparse_drop_chunks('table1', older_than => interval '2 years', newer_than => '2015-01-01'::timestamp);

-- test generalized deparsing function
SELECT * FROM tsl_test_deparse_scalar_func(schema_name => 'Foo', table_name => 'bar', option => false, "time" => timestamp '2019-09-10 11:08', message => 'This is a test message.');

SELECT * FROM tsl_test_deparse_named_scalar_func(schema_name => 'Foo', table_name => 'bar', option => false, "time" => timestamp '2019-09-10 11:08', message => 'This is a test message.');

SELECT * FROM tsl_test_deparse_composite_func(schema_name => 'Foo', table_name => 'bar', option => false, "time" => timestamp '2019-09-10 11:08', message => 'This is a test message.');

-- test errors handling
\set ON_ERROR_STOP 0

CREATE TEMP TABLE fail_table1(x INT);

SELECT _timescaledb_internal.get_tabledef('fail_table1');

CREATE INDEX my_fail_table1_idx ON fail_table1 USING BTREE(x);

SELECT _timescaledb_internal.get_tabledef('my_fail_table1_idx');

SELECT _timescaledb_internal.get_tabledef('non_existing');

CREATE TABLE row_sec(i INT);
ALTER TABLE row_sec ENABLE ROW LEVEL SECURITY;
SELECT _timescaledb_internal.get_tabledef('row_sec');
