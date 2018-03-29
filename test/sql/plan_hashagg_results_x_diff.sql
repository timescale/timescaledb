\set ECHO errors
\o /dev/null
SET client_min_messages = 'error';
\ir include/plan_hashagg_load.sql
RESET client_min_messages;
\o

--generate the results into two different files
SET client_min_messages = 'fatal';
\set ECHO none
--make output contain query results
\set PREFIX ''
\o :TEST_OUTPUT_DIR/results/plan_hashagg_optimized_results.out
SET timescaledb.disable_optimizations= 'off';
\ir include/plan_hashagg_query.sql
\o
\o :TEST_OUTPUT_DIR/results/plan_hashagg_unoptimized_results.out
SET timescaledb.disable_optimizations= 'on';
\ir include/plan_hashagg_query.sql
\o
RESET client_min_messages;

\! diff ${TEST_OUTPUT_DIR}/results/plan_hashagg_optimized_results.out ${TEST_OUTPUT_DIR}/results/plan_hashagg_unoptimized_results.out

SELECT 'Done';
