-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

\set ECHO errors
\set TEST_BASE_NAME agg_bookends
SELECT format('include/%s.sql', :'TEST_BASE_NAME') as "TEST_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED"
\gset
SELECT format('\! diff %s %s', :'TEST_RESULTS_OPTIMIZED', :'TEST_RESULTS_UNOPTIMIZED') as "DIFF_CMD"
\gset

--generate the results into two different files
SET client_min_messages = 'fatal';
\set ECHO none
--make output contain query results
\set PREFIX ''
\o :TEST_RESULTS_OPTIMIZED
SET timescaledb.disable_optimizations= 'off';
\ir :TEST_NAME
\o
\o :TEST_RESULTS_UNOPTIMIZED
SET timescaledb.disable_optimizations= 'on';
DROP TABLE IF EXISTS btest cascade;
DROP TABLE IF EXISTS btest_numeric cascade;
\ir :TEST_NAME
\o
RESET client_min_messages;

:DIFF_CMD

SELECT 'Done';
