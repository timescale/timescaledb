-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set TEST_BASE_NAME lastpoint_rewrite
SELECT format('include/%s_query.sql', :'TEST_BASE_NAME') AS "TEST_QUERY_NAME",
    format('%s/results/%s_results_unoptimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_UNOPTIMIZED",
    format('%s/results/%s_results_optimized.out', :'TEST_OUTPUT_DIR', :'TEST_BASE_NAME') AS "TEST_RESULTS_OPTIMIZED" \gset

SELECT format('\! diff -u --label "Unoptimized results" --label "Optimized results" %s %s', :'TEST_RESULTS_UNOPTIMIZED', :'TEST_RESULTS_OPTIMIZED') AS "DIFF_CMD" \gset

CREATE TABLE lastpoint(time timestamptz NOT NULL, device text NOT NULL, nullable_device text, v float8, tiebreak int)
WITH (tsdb.hypertable, tsdb.partition_column = 'time', tsdb.chunk_interval = '1 day',
    tsdb.segmentby = 'device', tsdb.orderby = 'time DESC');

INSERT INTO lastpoint
SELECT t, 'd' || d, CASE WHEN d % 7 = 0 THEN NULL ELSE 'd' || d END, (d * 37 + extract(epoch FROM t)::int / 3600 * 11) % 100, 1
FROM generate_series('2025-01-01'::timestamptz, '2025-01-05 23:00', '1 hour') t, generate_series(1, 20) d
WHERE d != 5 OR t < '2025-01-02';

-- Ties on time, broken by another column.
INSERT INTO lastpoint VALUES ('2025-01-05 23:00', 'd1', 'd1', 1000, 2), ('2025-01-05 23:00', 'd2', 'd2', -1000, 0);

SELECT count(compress_chunk(c)) FROM show_chunks('lastpoint', older_than => '2025-01-05'::timestamptz) c;
VACUUM ANALYZE lastpoint;

SET timescaledb.enable_lastpoint_rewrite TO on;

EXPLAIN (costs off, timing off, summary off)
SELECT DISTINCT ON (device) * FROM lastpoint ORDER BY device, time DESC;

-- Not rewritten: nullable key, no time order, volatile functions.
EXPLAIN (costs off, timing off, summary off)
SELECT DISTINCT ON (nullable_device) * FROM lastpoint ORDER BY nullable_device, time DESC;
EXPLAIN (costs off, timing off, summary off)
SELECT DISTINCT ON (device) * FROM lastpoint ORDER BY device, v DESC;
EXPLAIN (costs off, timing off, summary off)
SELECT DISTINCT ON (device) device, random() FROM lastpoint ORDER BY device, time DESC;

\o :TEST_RESULTS_OPTIMIZED
\ir :TEST_QUERY_NAME
\o

SET timescaledb.enable_lastpoint_rewrite TO off;

\o :TEST_RESULTS_UNOPTIMIZED
\ir :TEST_QUERY_NAME
\o

:DIFF_CMD

DROP TABLE lastpoint;
