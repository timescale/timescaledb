-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set PREFIX 'EXPLAIN (analyze, costs off, timing off, summary off)'

\set TEST_BASE_NAME cagg_planning
SELECT
    format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_results_baseline.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_BASELINE",
    format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_OPTIMIZED" \gset

SELECT format('\! diff -u --label Baseline --label Optimized %s %s', :'TEST_RESULTS_BASELINE', :'TEST_RESULTS_OPTIMIZED') AS "DIFF_CMD" \gset

SET timezone TO PST8PDT;

CREATE TABLE metrics(time timestamptz, device text, metric text, value float);
SELECT create_hypertable('metrics', 'time');

-- insert initial data to be in materialized part of cagg
INSERT INTO metrics SELECT '2020-01-01'::timestamptz + format('%s day', i::text)::interval, 'device 1', 'metric 1', i  FROM generate_series(0, 9, 1) g(i);

-- cagg with grouping only by time column
CREATE MATERIALIZED VIEW cagg1 WITH (timescaledb.continuous,timescaledb.materialized_only=false)
AS SELECT time_bucket('3 day', time), avg(value) FROM metrics GROUP BY 1;

CREATE MATERIALIZED VIEW cagg1_ordered_asc WITH (timescaledb.continuous,timescaledb.materialized_only=false)
AS SELECT time_bucket('3 day', time), avg(value) FROM metrics GROUP BY 1 ORDER BY 1;

CREATE MATERIALIZED VIEW cagg1_ordered_desc WITH (timescaledb.continuous,timescaledb.materialized_only=false)
AS SELECT time_bucket('3 day', time), avg(value) FROM metrics GROUP BY 1 ORDER BY 1 DESC;

-- cagg with grouping by device and time column
CREATE MATERIALIZED VIEW cagg2 WITH (timescaledb.continuous,timescaledb.materialized_only=false)
AS SELECT device, time_bucket('3 day', time), avg(value) FROM metrics GROUP BY device, 2;

-- cagg with first/last
CREATE MATERIALIZED VIEW cagg3 WITH (timescaledb.continuous,timescaledb.materialized_only=false)
AS SELECT time_bucket('3 day', time), first(value, time), last(value, time), array_agg(value ORDER BY value) AS array_asc, array_agg(value ORDER BY value DESC) AS array_desc FROM metrics GROUP BY device, 1;

-- insert more data to be in real-time part of cagg
INSERT INTO metrics SELECT '2020-01-01'::timestamptz + format('%s day', i::text)::interval, 'device 1', 'metric 1', i  FROM generate_series(10, 16, 1) g(i);

\ir :TEST_QUERY_NAME

\set ECHO none
\set PREFIX ''

SET timescaledb.enable_cagg_sort_pushdown TO off;
\o :TEST_RESULTS_BASELINE
\ir :TEST_QUERY_NAME
\o

RESET timescaledb.enable_cagg_sort_pushdown;
\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

\set ECHO all

-- diff baseline and optimized results
:DIFF_CMD

--dump & restore
\c postgres :ROLE_SUPERUSER

\! utils/pg_dump_aux_dump.sh dump/pg_dump.sql

\c :TEST_DBNAME
SET client_min_messages = ERROR;
CREATE EXTENSION timescaledb CASCADE;
RESET client_min_messages;

SELECT timescaledb_pre_restore();
\! utils/pg_dump_aux_restore.sh dump/pg_dump.sql
SELECT timescaledb_post_restore();
SELECT _timescaledb_functions.stop_background_workers();

-- Repeat tests after restore
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

\set PREFIX 'EXPLAIN (analyze, costs off, timing off, summary off)'

\set TEST_BASE_NAME cagg_planning
SELECT
    format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_results_baseline_after_restore.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_BASELINE_AFTER_RESTORE",
    format('%s/results/%s_results_optimized_after_restore.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_OPTIMIZED_AFTER_RESTORE" \gset

SELECT format('\! diff -u --label Baseline --label Optimized %s %s', :'TEST_RESULTS_BASELINE_AFTER_RESTORE', :'TEST_RESULTS_OPTIMIZED_AFTER_RESTORE') AS "DIFF_CMD" \gset

SET timezone TO PST8PDT;

\ir :TEST_QUERY_NAME

\set ECHO none
\set PREFIX ''

SET timescaledb.enable_cagg_sort_pushdown TO off;
\o :TEST_RESULTS_BASELINE_AFTER_RESTORE
\ir :TEST_QUERY_NAME
\o

RESET timescaledb.enable_cagg_sort_pushdown;
\o :TEST_RESULTS_OPTIMIZED_AFTER_RESTORE
\ir :TEST_QUERY_NAME
\o

\set ECHO all

-- diff baseline and optimized results
:DIFF_CMD

-- diff baseline before and after restore
SELECT format('\! diff -u --label Baseline --label Baseline_After_Restore %s %s', :'TEST_RESULTS_BASELINE', :'TEST_RESULTS_BASELINE_AFTER_RESTORE') AS "DIFF_CMD" \gset
:DIFF_CMD

-- diff optimized before and after restore
SELECT format('\! diff -u --label Optimized --label Optimized_After_Restore %s %s', :'TEST_RESULTS_OPTIMIZED', :'TEST_RESULTS_OPTIMIZED_AFTER_RESTORE') AS "DIFF_CMD" \gset
:DIFF_CMD
