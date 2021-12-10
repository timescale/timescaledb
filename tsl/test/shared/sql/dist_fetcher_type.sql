-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP off

-- Test that we use the correct type of remote data fetcher.
set timescaledb.remote_data_fetcher = 'auto';
select 1 x from distinct_on_distributed t1, distinct_on_distributed t2
where t1.id = t2.id + 1
limit 1;

set timescaledb.remote_data_fetcher = 'cursor';
select 1 x from distinct_on_distributed t1, distinct_on_distributed t2
where t1.id = t2.id + 1
limit 1;

explain (verbose, costs off)
select 1 x from distinct_on_distributed t1, distinct_on_distributed t2
where t1.id = t2.id + 1
limit 1;

set timescaledb.remote_data_fetcher = 'rowbyrow';
select 1 x from distinct_on_distributed t1, distinct_on_distributed t2
where t1.id = t2.id + 1
limit 1;

explain (verbose, costs off)
select 1 x from distinct_on_distributed t1, distinct_on_distributed t2
where t1.id = t2.id + 1
limit 1;

-- Check once again that 'auto' is used after 'rowbyrow'.
set timescaledb.remote_data_fetcher = 'auto';
select 1 x from distinct_on_distributed t1, distinct_on_distributed t2
where t1.id = t2.id + 1
limit 1;

reset timescaledb.remote_data_fetcher;

-- #3786 test for assertion failure in cursor_fetcher_rewind
SET jit TO off;
SELECT *
FROM devices AS d
WHERE
  EXISTS(
    SELECT 1
    FROM metrics_dist AS m,
      LATERAL(
        SELECT 1
        FROM insert_test it
        WHERE
          EXISTS(
            SELECT 1
            FROM dist_chunk_copy AS ref_2
            WHERE
              it.id IS NOT NULL AND
              EXISTS(SELECT d.name AS c0 FROM metrics_int WHERE NULL::TIMESTAMP <= m.time)
          )
      ) AS l
    WHERE d.name ~~ d.name
  )
ORDER BY 1,2;
RESET jit;
