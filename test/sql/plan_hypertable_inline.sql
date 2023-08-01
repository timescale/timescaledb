-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- test hypertable classification when query is in an inlineable function

\set PREFIX 'EXPLAIN (costs off)'

CREATE TABLE test (a int, b bigint NOT NULL);
SELECT create_hypertable('public.test', 'b', chunk_time_interval=>10);
INSERT INTO test SELECT i, i FROM generate_series(1, 20) i;

CREATE OR REPLACE FUNCTION test_f(_ts bigint)
RETURNS SETOF test LANGUAGE SQL STABLE
as $f$
   SELECT DISTINCT ON (a) * FROM test WHERE b >= _ts AND b <= _ts + 2
$f$;

-- plans must be the same in both cases
-- specifically, the first plan should not contain the parent hypertable
-- as that is a sign the pruning was not done successfully
:PREFIX SELECT * FROM test_f(5);

:PREFIX SELECT DISTINCT ON (a) * FROM test WHERE b >= 5 AND b <= 5 + 2;


-- test with FOR UPDATE
CREATE OR REPLACE FUNCTION test_f(_ts bigint)
RETURNS SETOF test LANGUAGE SQL STABLE
as $f$
   SELECT * FROM test WHERE b >= _ts AND b <= _ts + 2 FOR UPDATE
$f$;


-- pruning should not be done by TimescaleDb in this case
-- specifically, the parent hypertable must exist in the output plan
:PREFIX SELECT * FROM test_f(5);

:PREFIX SELECT * FROM test WHERE b >= 5 AND b <= 5 + 2 FOR UPDATE;

-- test with CTE
-- these cases are just to make sure we're everything is alright with
-- the way we identify hypertables to prune chunks - we abuse ctename
-- for this purpose. So double-check if we're not breaking plans
-- with CTEs here.
CREATE OR REPLACE FUNCTION test_f(_ts bigint)
RETURNS SETOF test LANGUAGE SQL STABLE
as $f$
   WITH ct AS MATERIALIZED (
      SELECT DISTINCT ON (a) * FROM test WHERE b >= _ts AND b <= _ts + 2
   )
   SELECT * FROM ct
$f$;

:PREFIX SELECT * FROM test_f(5);

:PREFIX
WITH ct AS MATERIALIZED (
   SELECT DISTINCT ON (a) * FROM test WHERE b >= 5 AND b <= 5 + 2
)
SELECT * FROM ct;

-- CTE within CTE
:PREFIX
WITH ct AS MATERIALIZED (
   SELECT * FROM test_f(5)
)
SELECT * FROM ct;

-- CTE within NO MATERIALIZED CTE
:PREFIX
WITH ct AS NOT MATERIALIZED (
   SELECT * FROM test_f(5)
)
SELECT * FROM ct;
