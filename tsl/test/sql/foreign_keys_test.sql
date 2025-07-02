-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

\set TEST_BASE_NAME foreign_keys
SELECT format('include/%s.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_results_generic.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_GENERIC",
    format('%s/results/%s_results_custom.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_CUSTOM" \gset

SELECT format('\! diff -u --label "Generic plan results" --label "Custom plan results" %s %s', :'TEST_RESULTS_GENERIC', :'TEST_RESULTS_CUSTOM') AS "DIFF_CMD" \gset

set plan_cache_mode to force_generic_plan;
\o :TEST_RESULTS_GENERIC
\ir :TEST_QUERY_NAME
\o
reset plan_cache_mode;

\set TEST_DBNAME_2 :TEST_DBNAME _2
CREATE DATABASE :TEST_DBNAME_2;
\c :TEST_DBNAME_2 :ROLE_SUPERUSER

SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb CASCADE;
RESET client_min_messages;

set plan_cache_mode to force_custom_plan;

\o :TEST_RESULTS_CUSTOM
\ir :TEST_QUERY_NAME
\o

\c :TEST_DBNAME :ROLE_SUPERUSER
reset plan_cache_mode;
DROP DATABASE :TEST_DBNAME_2 WITH (force);

-- compare outputs for generic vs. custom plans for FK check prepared statements
:DIFF_CMD

