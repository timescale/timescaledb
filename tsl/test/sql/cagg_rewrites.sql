-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set TEST_BASE_NAME cagg_rewrites
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') AS "TEST_LOAD_NAME",
    format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('include/%s_error.sql', :'TEST_BASE_NAME') AS "TEST_ERROR_NAME",
    format('%s/results/%s_results_rewritten.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_REWRITTEN",
    format('%s/results/%s_results_not_rewritten.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_NOT_REWRITTEN" \gset

SELECT format('\! diff -u --label "Original results" --label "Rewritten results" %s %s', :'TEST_RESULTS_NOT_REWRITTEN', :'TEST_RESULTS_REWRITTEN') AS "DIFF_CMD" \gset

\ir :TEST_LOAD_NAME

SET timescaledb.cagg_rewrites_debug_info = 1;
SET timescaledb.enable_cagg_rewrites = 1;

-- Make sure Cagg was used in a plan for an eligible query
EXPLAIN (buffers off, costs off, timing off, summary off) SELECT time_bucket(INTERVAL '2 day', day) AS bucket,
   count(device_id)
FROM conditions
GROUP BY bucket
ORDER BY 1
LIMIT 3;

-- run tests on queries eligible for Cagg rewrites and diff results
\o :TEST_RESULTS_REWRITTEN
\ir :TEST_QUERY_NAME
\o

SET timescaledb.enable_cagg_rewrites = 0;
SET timescaledb.cagg_rewrites_debug_info = 0;

\o :TEST_RESULTS_NOT_REWRITTEN
\ir :TEST_QUERY_NAME
\o

-- compare results for original queries vs. queries rewritten with Caggs
:DIFF_CMD

SET timescaledb.cagg_rewrites_debug_info = 1;
-- Test queries ineligible for Cagg rewrites, display diagnostics on why
\ir :TEST_ERROR_NAME

RESET timescaledb.cagg_rewrites_debug_info;
RESET timescaledb.enable_cagg_rewrites;

DROP MATERIALIZED VIEW cagg_on_cagg1 CASCADE;
DROP MATERIALIZED VIEW cagg1 CASCADE;
DROP MATERIALIZED VIEW cagg2 CASCADE;
DROP MATERIALIZED VIEW cagg3 CASCADE;
DROP MATERIALIZED VIEW cagg_join CASCADE;
DROP MATERIALIZED VIEW cagg_more_conds CASCADE;
DROP MATERIALIZED VIEW cagg_view CASCADE;
DROP MATERIALIZED VIEW cagg_lateral CASCADE;

DROP TABLE conditions CASCADE;
DROP TABLE conditions_dup CASCADE;
DROP TABLE conditions_custom CASCADE;
DROP VIEW devices_view CASCADE;
DROP TABLE devices CASCADE;
DROP TABLE location CASCADE;
