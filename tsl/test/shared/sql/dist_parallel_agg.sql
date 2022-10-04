-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test that for parallel-safe aggregate function a parallel plan is generated
-- on data nodes, and for unsafe it is not. We use a manually created safe
-- function and not a builtin one, to check that we can in fact create a
-- function that is parallelized, to prevent a false negative (i.e. it's not
-- parallelized, but for a different reason, not because it's unsafe).

-- Create a relatively big table on one data node to test parallel plans and
-- avoid flakiness.
create table metrics_dist1(like metrics_dist);
select table_name from create_distributed_hypertable('metrics_dist1', 'time', 'device_id',
    data_nodes => '{"data_node_1"}');
insert into metrics_dist1 select * from metrics_dist order by metrics_dist limit 20000;

-- Start transaction in order to keep same connection
-- during the test and avoid connection cache invalidations
begin;

\set safe 'create or replace aggregate ts_debug_shippable_safe_count(*) (sfunc = int8inc, combinefunc=int8pl, stype = bigint, initcond = 0, parallel = safe);'
\set unsafe 'create or replace aggregate ts_debug_shippable_unsafe_count(*) (sfunc = int8inc, combinefunc=int8pl, stype = bigint, initcond = 0, parallel = unsafe);'

:safe
call distributed_exec(:'safe');
:unsafe
call distributed_exec(:'unsafe');

call distributed_exec($$ set parallel_tuple_cost = 0; $$);
call distributed_exec($$ set parallel_setup_cost = 0; $$);
call distributed_exec($$ set max_parallel_workers_per_gather = 1; $$);

set timescaledb.enable_remote_explain = 1;
set enable_partitionwise_aggregate = 1;

\set analyze 'explain (analyze, verbose, costs off, timing off, summary off)'

:analyze
select count(*) from metrics_dist1;

:analyze
select ts_debug_shippable_safe_count(*) from metrics_dist1;

:analyze
select ts_debug_shippable_unsafe_count(*) from metrics_dist1;

commit;
