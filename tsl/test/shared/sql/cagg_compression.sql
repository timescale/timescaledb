-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT
       format('include/%s_setup.sql', :'TEST_BASE_NAME') as "TEST_SETUP_NAME",
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/shared/results/%s_results_source.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_SOURCE",
       format('%s/shared/results/%s_results_target.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_TARGET"
\gset
SELECT format('\! diff -u --label "Source results" --label "Target results" %s %s', :'TEST_RESULTS_SOURCE', :'TEST_RESULTS_TARGET') as "DIFF_CMD"
\gset

-- Setup
\ir :TEST_SETUP_NAME

\o :TEST_RESULTS_SOURCE
\set TEST_TABLE1 'metrics_compressed_summary'
\set TEST_TABLE2 'devices'
\ir :TEST_QUERY_NAME

\set TEST_TABLE1 'metrics_compressed_summary'
\set TEST_TABLE2 'all_devices'
\ir :TEST_QUERY_NAME
\o

\o :TEST_RESULTS_TARGET
\set TEST_TABLE1 'metrics_summary'
\set TEST_TABLE2 'devices'
\ir :TEST_QUERY_NAME

\set TEST_TABLE1 'metrics_summary'
\set TEST_TABLE2 'all_devices'
\ir :TEST_QUERY_NAME
\o

-- Compare results of CAgg over Compressed and Uncompressed hypertables
:DIFF_CMD

\o :TEST_RESULTS_SOURCE
\set TEST_TABLE1 'metrics_compressed_summary'
\set TEST_TABLE2 'devices'
\ir :TEST_QUERY_NAME

\set TEST_TABLE1 'metrics_compressed_summary'
\set TEST_TABLE2 'all_devices'
\ir :TEST_QUERY_NAME
\o

\o :TEST_RESULTS_TARGET
\set TEST_TABLE1 '(SELECT device_id, time_bucket(INTERVAL $$1 week$$, time) AS bucket, AVG(v2), MIN(v2), MAX(v2) FROM metrics_compressed GROUP BY device_id, bucket)'
\set TEST_TABLE2 'devices'
\ir :TEST_QUERY_NAME

\set TEST_TABLE1 '(SELECT device_id, time_bucket(INTERVAL $$1 week$$, time) AS bucket, AVG(v2), MIN(v2), MAX(v2) FROM metrics_compressed GROUP BY device_id, bucket)'
\set TEST_TABLE2 'all_devices'
\ir :TEST_QUERY_NAME
\o

-- Compare results between CAggs and Original View (to check PG and TSDB machinery)
:DIFF_CMD

-- Decompres 1 chunk from cagg
SELECT CASE WHEN res is NULL THEN NULL
            ELSE 'decompress'
       END as dec
FROM ( SELECT decompress_chunk(ch) res FROM show_chunks('metrics_compressed_summary') ch ORDER BY ch LIMIT 1) q;

-- Run tests and compare results
\o :TEST_RESULTS_SOURCE
\set TEST_TABLE1 'metrics_compressed_summary'
\set TEST_TABLE2 'devices'
\ir :TEST_QUERY_NAME

\set TEST_TABLE1 'metrics_compressed_summary'
\set TEST_TABLE2 'all_devices'
\ir :TEST_QUERY_NAME
\o

\o :TEST_RESULTS_TARGET
\set TEST_TABLE1 'metrics_summary'
\set TEST_TABLE2 'devices'
\ir :TEST_QUERY_NAME

\set TEST_TABLE1 'metrics_summary'
\set TEST_TABLE2 'all_devices'
\ir :TEST_QUERY_NAME
\o

-- Compare results of CAgg (partial compressed) over Compressed hypertables
:DIFF_CMD

-- Teardown
DROP MATERIALIZED VIEW metrics_compressed_summary;
DROP MATERIALIZED VIEW metrics_summary;
DROP VIEW all_devices;
DROP TABLE extra_devices;
