--make sure diff only has explain output not result output
\! diff ${TEST_OUTPUT_DIR}/results/sql_query_results_optimized.out ${TEST_OUTPUT_DIR}/results/sql_query_results_unoptimized.out
