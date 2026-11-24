-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set TEST_BASE_NAME compression_sorted_merge_segmentwise
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') AS "TEST_LOAD_NAME",
    format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_UNOPTIMIZED",
    format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_OPTIMIZED" \gset

SELECT format('\! diff -u --label "Unoptimized results" --label "Optimized results" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') AS "DIFF_CMD" \gset

-- Increase the working memory limit slightly, otherwise the batch sorted merge
-- will be penalized for segmentby cardinalities larger than 100, where it is
-- still faster than sort.
SET work_mem to '16MB';

SET timezone = 'UTC';

\ir :TEST_LOAD_NAME

SET timescaledb.enable_decompression_sorted_merge = 1;
SET timescaledb.debug_require_batch_sorted_merge TO 'force';
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o
RESET timescaledb.debug_require_batch_sorted_merge;

SET timescaledb.enable_decompression_sorted_merge = 0;
\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o
RESET timescaledb.enable_decompression_sorted_merge;

-- compare results with and without batch sorted merge
:DIFF_CMD

RESET timezone;
RESET work_mem;

drop table t2_seg cascade;
drop table t2_multi cascade;
drop table t2_multi_reg cascade;
