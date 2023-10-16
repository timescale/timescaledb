-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
\set ON_ERROR_STOP 0
\set VERBOSITY default
SET client_min_messages TO error;

\set TEST_BASE_NAME ts_merge
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') AS "TEST_LOAD_NAME",
    format('include/%s_load_ht.sql', :'TEST_BASE_NAME') AS "TEST_LOAD_HT_NAME",
    format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_results.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_WITH_HYPERTABLE",
    format('%s/results/%s_ht_results.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_WITH_NO_HYPERTABLE" \gset

SELECT format('\! diff -u --label "Base pg table results" --label "Hyperatable results" %s %s', :'TEST_RESULTS_WITH_HYPERTABLE', :'TEST_RESULTS_WITH_NO_HYPERTABLE') AS "DIFF_CMD" \gset

\ir :TEST_LOAD_NAME

-- run tests on normal table
\o :TEST_RESULTS_WITH_NO_HYPERTABLE
\ir :TEST_QUERY_NAME
\o

\ir :TEST_LOAD_HT_NAME

-- run tests on hypertable
\o :TEST_RESULTS_WITH_HYPERTABLE
\ir :TEST_QUERY_NAME
\o

:DIFF_CMD
