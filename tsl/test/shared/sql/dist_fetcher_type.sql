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
