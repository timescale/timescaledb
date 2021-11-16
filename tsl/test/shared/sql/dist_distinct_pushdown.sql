-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test DISTINCT ON pushdown.


-- The case with LIMIT serves as a reference.
select ts, id from distinct_on_hypertable order by id, ts desc limit 1;
select ts, id from distinct_on_distributed order by id, ts desc limit 1;


-- DISTINCT ON should match the above LIMIT for the first id.
select distinct on (id) ts, id from distinct_on_hypertable order by id, ts desc;

select distinct on (id) ts, id from distinct_on_distributed order by id, ts desc;

explain (costs off, verbose)
select distinct on (id) ts, id from distinct_on_distributed order by id, ts desc;


-- A case where we have a filter on the DISTINCT ON column.
select distinct on (id) ts, id from distinct_on_distributed where id in ('0', '1') order by id, ts desc;

explain (costs off, verbose)
select distinct on (id) ts, id from distinct_on_distributed where id in ('0', '1') order by id, ts desc;


-- A somewhat dumb case where the DISTINCT ON column is deduced to be constant
-- and not added to pathkeys.
select distinct on (id) ts, id from distinct_on_distributed where id in ('0') order by id, ts desc;

explain (costs off, verbose)
select distinct on (id) ts, id from distinct_on_distributed where id in ('0') order by id, ts desc;


-- All above but with disabled local sort, to try to force more interesting plans where the sort
-- is pushed down.
set enable_sort = 0;

select ts, id from distinct_on_distributed order by id, ts desc limit 1;

explain (costs off, verbose)
select ts, id from distinct_on_distributed order by id, ts desc limit 1;

select distinct on (id) ts, id from distinct_on_distributed order by id, ts desc;

explain (costs off, verbose)
select distinct on (id) ts, id from distinct_on_distributed order by id, ts desc;

select distinct on (id) ts, id from distinct_on_distributed where id in ('0', '1') order by id, ts desc;

explain (costs off, verbose)
select distinct on (id) ts, id from distinct_on_distributed where id in ('0', '1') order by id, ts desc;

select distinct on (id) ts, id from distinct_on_distributed where id in ('0') order by id, ts desc;

explain (costs off, verbose)
select distinct on (id) ts, id from distinct_on_distributed where id in ('0') order by id, ts desc;

reset enable_sort;