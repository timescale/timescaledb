-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Exercise the GatherMerge -> Sort -> Append -> Partial Aggregate plan.

\set prefix 'explain (costs off, analyze, timing off, summary off)'

set parallel_setup_cost = 0;

set enable_hashagg to off;

:prefix
select count(*) from metrics group by v0;

reset enable_hashagg;

:prefix
select count(*) from metrics group by v0;

reset parallel_setup_cost;
