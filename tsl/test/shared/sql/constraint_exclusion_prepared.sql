-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT
       format('include/%s.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/shared/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED",
       format('%s/shared/results/%s_results_uncompressed.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNCOMPRESSED",
       format('%s/shared/results/%s_results_compressed.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_COMPRESSED"
\gset
SELECT format('\! diff -u --label "Uncompressed results" --label "Compressed results" %s %s', :'TEST_RESULTS_UNCOMPRESSED', :'TEST_RESULTS_COMPRESSED') as "DIFF_CMD"
\gset

-- get EXPLAIN output for all variations
\set PREFIX 'EXPLAIN (analyze, costs off, timing off, summary off)'
\set PREFIX_VERBOSE 'EXPLAIN (analyze, costs off, timing off, summary off, verbose)'

SET work_mem TO '64MB';

-- In the following test cases, we test that certain indexes are used. By using the
-- timescaledb.enable_decompression_sorted_merge optimization, we are pushing a sort node
-- below the DecompressChunk node, which operates on the batches. This could lead to flaky
-- tests because the input data is small and PostgreSQL switches from IndexScans to
-- SequentialScans. Disable the optimization for the following tests to ensure we have
-- stable query plans in all CI environments.
SET timescaledb.enable_decompression_sorted_merge TO 'off';
SET max_parallel_workers_per_gather TO '0';

-- disable incremental sort to avoid flaky results
SET enable_incremental_sort TO 'off';
-- disable memoize node to avoid flaky results
SET enable_memoize TO 'off';

\set TEST_TABLE 'metrics'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_compressed'
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space_compressed'
\ir :TEST_QUERY_NAME


-- get results for all the queries
-- run queries on uncompressed hypertable and store result
\set PREFIX ''
\set PREFIX_VERBOSE ''
\set ECHO none
SET client_min_messages TO error;

-- run queries on compressed hypertable and store result
\set TEST_TABLE 'metrics'
\o :TEST_RESULTS_UNCOMPRESSED
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_compressed'
\o :TEST_RESULTS_COMPRESSED
\ir :TEST_QUERY_NAME
\o

-- diff compressed and uncompressed results
:DIFF_CMD

-- do the same for space partitioned hypertable
\set TEST_TABLE 'metrics_space'
\o :TEST_RESULTS_UNCOMPRESSED
\ir :TEST_QUERY_NAME
\set TEST_TABLE 'metrics_space_compressed'
\o :TEST_RESULTS_COMPRESSED
\ir :TEST_QUERY_NAME
\o

-- diff compressed and uncompressed results
:DIFF_CMD

