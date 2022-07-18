-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP off

-- Test that we use the correct type of remote data fetcher.
set timescaledb.remote_data_fetcher = 'auto';
select 1 x from distinct_on_distributed t1, distinct_on_distributed t2
where t1.id = t2.id + 1
limit 1;

-- This query should choose row-by-row fetcher.
select 1 x from distinct_on_distributed t1
limit 1;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 x from distinct_on_distributed t1
limit 1;

set timescaledb.remote_data_fetcher = 'cursor';
select 1 x from distinct_on_distributed t1, distinct_on_distributed t2
where t1.id = t2.id + 1
limit 1;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 x from distinct_on_distributed t1, distinct_on_distributed t2
where t1.id = t2.id + 1
limit 1;

-- This query can't work with rowbyrow fetcher.
set timescaledb.remote_data_fetcher = 'rowbyrow';
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


-- Row by row fetcher should fail on a custom type that has no binary
-- serialization.
set timescaledb.remote_data_fetcher = 'rowbyrow';

explain (analyze, verbose, costs off, timing off, summary off)
select time, txn_id, val, substring(info for 20) from disttable_with_ct;

-- Cursor fetcher should be chosen automatically if we have a data type with no
-- binary serialization.
set timescaledb.remote_data_fetcher = 'auto';

explain (analyze, verbose, costs off, timing off, summary off)
select * from disttable_with_ct;

-- Row by row fetcher with bytea data
set timescaledb.remote_data_fetcher = 'rowbyrow';

explain (analyze, verbose, costs off, timing off, summary off)
select * from disttable_with_bytea;
select * from disttable_with_bytea;

-- Cursor fetcher with bytea data
set timescaledb.remote_data_fetcher = 'cursor';

explain (analyze, verbose, costs off, timing off, summary off)
select * from disttable_with_bytea;
select * from disttable_with_bytea;

-- #4515 test for assertion failure in row_by_row_fetcher_close
SET timescaledb.remote_data_fetcher = 'rowbyrow';
SELECT *
FROM
  conditions ref_0
WHERE EXISTS (
  SELECT FROM
    distinct_on_distributed,
    LATERAL (
      SELECT *
      FROM pg_class,
      LATERAL (
        SELECT ref_0.device FROM pg_class WHERE false LIMIT 1) as lat_1
      ) as lat_2
  WHERE (SELECT 1 FROM pg_class LIMIT 1) >= ref_0.device
);

-- #4518
-- test error handling for queries with multiple distributed hypertables
SET timescaledb.remote_data_fetcher = 'rowbyrow';
SELECT * FROM
  conditions_dist1 ref_0
WHERE EXISTS (
  SELECT FROM
    distinct_on_distributed as ref_1,
    LATERAL (select * from metrics as ref_2) as subq_3
  WHERE
    (SELECT device_id FROM metrics_compressed limit 1 offset 3) >= ref_0.device
);
SET timescaledb.remote_data_fetcher = 'auto';
SELECT * FROM
  conditions_dist1 ref_0
WHERE EXISTS (
  SELECT FROM
    distinct_on_distributed as ref_1,
    LATERAL (select * from metrics as ref_2) as subq_3
  WHERE
    (SELECT device_id FROM metrics_compressed limit 1 offset 3) >= ref_0.device
)
ORDER BY 1,2;
