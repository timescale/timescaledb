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
SELECT * FROM tsl_test_deparse_drop_chunks('2019-01-01'::timestamptz, 'test_table', 'test_schema', cascade => false, verbose => true);
SELECT * FROM tsl_test_deparse_drop_chunks(interval '1 day', table_name => 'weird nAme\\#^.', cascade_to_materializations => true, cascade => true);
SELECT * FROM tsl_test_deparse_drop_chunks(newer_than => 12345);
SELECT * FROM tsl_test_deparse_drop_chunks(older_than => interval '2 years', newer_than => '2015-01-01'::timestamp);
