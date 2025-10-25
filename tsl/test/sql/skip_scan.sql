-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- need superuser to adjust statistics in load script
\c :TEST_DBNAME :ROLE_SUPERUSER

\set TEST_BASE_NAME skip_scan
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') AS "TEST_LOAD_NAME",
    format('include/%s_multi_load.sql', :'TEST_BASE_NAME') AS "TEST_MULTI_LOAD_NAME",
    format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_UNOPTIMIZED",
    format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_OPTIMIZED" \gset

SELECT format('\! diff -u --label "Unoptimized results" --label "Optimized results" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') AS "DIFF_CMD" \gset

\ir :TEST_LOAD_NAME

-- run tests on normal table and diff results
\set TABLE skip_scan
\set PREFIX ''
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

SET timescaledb.enable_skipscan TO false;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o
RESET timescaledb.enable_skipscan;

-- compare SkipScan results on normal table
:DIFF_CMD

-- run tests on hypertable and diff results
\set TABLE skip_scan_ht
\set PREFIX ''
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

SET timescaledb.enable_skipscan TO false;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o
RESET timescaledb.enable_skipscan;

-- compare SkipScan results on hypertable
:DIFF_CMD

-- run tests on compressed hypertable with different compression settings and diff results
SELECT format('include/%s_comp_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME" \gset

\set TABLE skip_scan_htc
\set PREFIX ''
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

SET timescaledb.enable_compressed_skipscan TO false;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o
RESET timescaledb.enable_compressed_skipscan;

-- compare SkipScan results on compressed hypertable
:DIFF_CMD

-- run tests on compressed hypertable with different layouts of compressed chunks
SELECT format('include/%s_load_comp_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME" \gset

\set TABLE skip_scan_htcl
\set PREFIX ''
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

SET timescaledb.enable_compressed_skipscan TO false;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o
RESET timescaledb.enable_compressed_skipscan;

-- compare SkipScan results on compressed hypertable
:DIFF_CMD

-- run tests on compressed hypertable with different layouts of compressed chunks
SELECT format('include/%s_multi_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME" \gset

-- run multikey SkipScan tests

\ir :TEST_MULTI_LOAD_NAME
\set PREFIX ''

-- make sure multikey SkipScan results are correct
\set TABLE mskip_scan
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

SET timescaledb.enable_multikey_skipscan TO false;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o
RESET timescaledb.enable_multikey_skipscan;

-- compare SkipScan results on table
:DIFF_CMD

\set TABLE mskip_scan_ht
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

SET timescaledb.enable_multikey_skipscan TO false;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o
RESET timescaledb.enable_multikey_skipscan;

-- compare SkipScan results on hypertable
:DIFF_CMD

\set TABLE mskip_scan_htc
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

SET timescaledb.enable_multikey_skipscan TO false;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o
RESET timescaledb.enable_multikey_skipscan;

-- compare SkipScan results on compressed hypertable
:DIFF_CMD

-- make sure multikey SkipScan is applied correctly
SET timescaledb.debug_skip_scan_info TO true;

\set TABLE mskip_scan
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

\set TABLE mskip_scan_ht
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

\set TABLE mskip_scan_htc
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

RESET timescaledb.debug_skip_scan_info;
