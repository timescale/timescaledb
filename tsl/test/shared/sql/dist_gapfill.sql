-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

\set TEST_BASE_NAME dist_gapfill
\set TEST_METRICS_NAME gapfill_metrics
SELECT format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/shared/results/%s_singlenode.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_SINGLENODE",
    format('%s/shared/results/%s_partitionwise_off.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_PARTITIONWISE_OFF",
    format('%s/shared/results/%s_partitionwise_on.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_PARTITIONWISE_ON",
    format('include/%s_query.sql', :'TEST_METRICS_NAME') AS "TEST_METRICS_QUERY_NAME",
    format('%s/shared/results/%s_nohyper.out', :'TEST_OUTPUT_DIR', :'TEST_METRICS_NAME') AS "TEST_METRICS_NOHYPER",
    format('%s/shared/results/%s_partitionwise_off.out', :'TEST_OUTPUT_DIR', :'TEST_METRICS_NAME') AS "TEST_METRICS_PARTITIONWISE_OFF" \gset

SELECT format('\! diff %s %s', :'TEST_SINGLENODE', :'TEST_PARTITIONWISE_OFF') AS "DIFF_CMD_PARTITIONWISE_OFF",
    format('\! diff %s %s', :'TEST_SINGLENODE', :'TEST_PARTITIONWISE_ON') AS "DIFF_CMD_PARTITIONWISE_ON",
    format('\! diff %s %s', :'TEST_METRICS_NOHYPER', :'TEST_METRICS_PARTITIONWISE_OFF') AS "DIFF_CMD_METRICS_PARTITIONWISE_OFF" \gset

-- Non-distributed hypertables
-- dist_gapfill_query
\set CONDITIONS conditions

\set ECHO all
\ir :TEST_QUERY_NAME
\set ECHO errors
\o :TEST_SINGLENODE
\ir :TEST_QUERY_NAME
\o

-- Run gapfill on a table as in gapfill.sql, where the result is verified

\set METRICS metrics_int

\o :TEST_METRICS_NOHYPER
\ir :TEST_METRICS_QUERY_NAME
\o

-- Distributed hypertables with three data nodes

-- dist_gapfill_query

\set CONDITIONS conditions_dist

SET enable_partitionwise_aggregate = 'off';
\o :TEST_PARTITIONWISE_OFF
\ir :TEST_QUERY_NAME
\o

SET enable_partitionwise_aggregate = 'on';
\o :TEST_PARTITIONWISE_ON
\ir :TEST_QUERY_NAME
\o

SET enable_partitionwise_aggregate = 'off';

-- gapfill_metrics_query
\set METRICS metrics_int_dist

\o :TEST_METRICS_PARTITIONWISE_OFF
\ir :TEST_METRICS_QUERY_NAME
\o

\set ECHO all

:DIFF_CMD_PARTITIONWISE_OFF
:DIFF_CMD_PARTITIONWISE_ON
:DIFF_CMD_METRICS_PARTITIONWISE_OFF

\set ECHO errors

-- Distributed hypertables with one data nodes

-- dist_gapfill_query
\set CONDITIONS conditions_dist1

SET enable_partitionwise_aggregate = 'off';
\o :TEST_PARTITIONWISE_OFF
\ir :TEST_QUERY_NAME
\o

SET enable_partitionwise_aggregate = 'on';
\o :TEST_PARTITIONWISE_ON
\ir :TEST_QUERY_NAME
\o

SET enable_partitionwise_aggregate = 'off';

-- gapfill_metrics_query
\set METRICS metrics_int_dist1

\o :TEST_METRICS_PARTITIONWISE_OFF
\ir :TEST_METRICS_QUERY_NAME
\o

\set ECHO all

:DIFF_CMD_PARTITIONWISE_OFF
:DIFF_CMD_PARTITIONWISE_ON
:DIFF_CMD_METRICS_PARTITIONWISE_OFF
