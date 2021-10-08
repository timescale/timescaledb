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

-- disable memoize node to make EXPLAIN output comparable between PG14 and previous versions
SELECT CASE WHEN current_setting('server_version_num')::int/10000 >= 14 THEN set_config('enable_memoize','off',false) ELSE 'off' END AS enable_memoize;

-- get EXPLAIN output for all variations
\set PREFIX 'EXPLAIN (analyze, costs off, timing off, summary off)'
\set PREFIX_VERBOSE 'EXPLAIN (analyze, costs off, timing off, summary off, verbose)'

set work_mem to '64MB';
-- disable incremental sort here to make plans comparable to PG < 13
SELECT CASE WHEN current_setting('server_version_num')::int/10000 >= 13 THEN set_config('enable_incremental_sort','off',false) ELSE 'off' END;

set max_parallel_workers_per_gather to 0;
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

