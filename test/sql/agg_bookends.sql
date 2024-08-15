-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set TEST_BASE_NAME agg_bookends
SELECT format('include/%s_load.sql', :'TEST_BASE_NAME') as "TEST_LOAD_NAME",
       format('include/%s_query.sql', :'TEST_BASE_NAME') as "TEST_QUERY_NAME",
       format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_OPTIMIZED",
       format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') as "TEST_RESULTS_UNOPTIMIZED"
\gset
SELECT format('\! diff -u  --label "Unoptimized result" --label "Optimized result" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') as "DIFF_CMD"
\gset

\set PREFIX 'EXPLAIN (analyze, costs off, timing off, summary off)'
\ir :TEST_LOAD_NAME
\ir :TEST_QUERY_NAME

-- we want test results as part of the output too to make sure we produce correct output
\set PREFIX ''
\ir :TEST_QUERY_NAME

-- diff results with optimizations disabled and enabled
\o :TEST_RESULTS_UNOPTIMIZED
SET timescaledb.enable_optimizations TO false;
\ir :TEST_QUERY_NAME
\o

\o :TEST_RESULTS_OPTIMIZED
SET timescaledb.enable_optimizations TO true;
\ir :TEST_QUERY_NAME
\o

:DIFF_CMD

-- Test partial aggregation
CREATE TABLE partial_aggregation (time timestamptz NOT NULL, quantity numeric, longvalue text);
SELECT schema_name, table_name, created FROM create_hypertable('partial_aggregation', 'time');

INSERT INTO partial_aggregation VALUES('2018-01-20T09:00:43', NULL, NULL);
INSERT INTO partial_aggregation VALUES('2018-01-20T09:00:43', NULL, NULL);
INSERT INTO partial_aggregation VALUES('2019-01-20T09:00:43', 1, 'hello');
INSERT INTO partial_aggregation VALUES('2019-01-20T09:00:43', 2, 'world');
INSERT INTO partial_aggregation VALUES('2020-01-20T09:00:43', 3, 'some');
INSERT INTO partial_aggregation VALUES('2020-01-20T09:00:43', 3, 'more');
INSERT INTO partial_aggregation VALUES('2021-01-20T09:00:43', 3, 'some');
INSERT INTO partial_aggregation VALUES('2021-01-20T09:00:43', 3, 'more');
INSERT INTO partial_aggregation VALUES('2022-01-20T09:00:43', 4, 'words');
INSERT INTO partial_aggregation VALUES('2022-01-20T09:00:43', 5, 'words');
INSERT INTO partial_aggregation VALUES('2023-01-20T09:00:43', 6, 'words');
INSERT INTO partial_aggregation VALUES('2023-01-20T09:00:43', 7, 'words');

-- Use enable_partitionwise_aggregate to create partial aggregates per chunk
SET enable_partitionwise_aggregate = ON;

SELECT
    format('SELECT %3$s, %1$s FROM partial_aggregation WHERE %2$s GROUP BY %3$s ORDER BY by 1, 2;',
            function, condition, grouping)
FROM
    unnest(array[
            'first(time, quantity), last(time, quantity)',
            'last(longvalue, quantity)',
            'last(quantity, longvalue)',
            'last(quantity, time)',
            'last(time, longvalue)']) AS function,
    unnest(array[
            'true',
            $$time < '2021-01-01'$$,
            'quantity is null',
            'quantity is not null',
            'quantity > 3']) AS condition,
    unnest(array[
            '777::text' /* dummy grouping column */,
            'longvalue',
            'quantity',
            $$time_bucket('1 year', time)$$,
            $$time_bucket('3 year', time)$$]) AS grouping
\gexec

SET enable_partitionwise_aggregate = OFF;

