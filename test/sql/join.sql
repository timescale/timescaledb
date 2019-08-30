-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- this test suite is based on the postgres join tests
--
-- the tests have been adjusted to work with hypertables
-- statements that would generate an error have been commented out
-- because errors don't play nicely with psql output redirection
-- plan output has been disabled, because we are not interested in
-- the actual plans produced but in the correctness of the results

-- we need superuser because some of the tests modify statistics
\c :TEST_DBNAME :ROLE_SUPERUSER

\set TEST_BASE_NAME join
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED"
\gset
SELECT format('\! diff -u --label "Unoptimized results" --label "Optimized results" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') as "DIFF_CMD"
\gset

set client_min_messages to warning;
\ir :TEST_LOAD_NAME

\set PREFIX ''
\set ECHO errors
-- get results with optimizations disabled
\o :TEST_RESULTS_UNOPTIMIZED
SET timescaledb.disable_optimizations TO true;
\ir :TEST_QUERY_NAME
\o

-- get query results with all optimizations
\o :TEST_RESULTS_OPTIMIZED
SET timescaledb.disable_optimizations TO false;
\ir :TEST_QUERY_NAME
\o

:DIFF_CMD
