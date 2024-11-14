-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test parameterized vector aggregation plans.


create table pvagg(s int, a int);

select create_hypertable('pvagg', 'a', chunk_time_interval => 1000);

insert into pvagg select 1, generate_series(1, 999);
insert into pvagg select 2, generate_series(1001, 1999);

alter table pvagg set (timescaledb.compress, timescaledb.compress_segmentby = 's');

select count(compress_chunk(x)) from show_chunks('pvagg') x;

analyze pvagg;

-- Uncomment to generate reference
--set timescaledb.enable_vectorized_aggregation to off;


explain (verbose, costs off)
select * from unnest(array[0, 1, 2]::int[]) x, lateral (select sum(a) from pvagg where s = x) xx;

select * from unnest(array[0, 1, 2]::int[]) x, lateral (select sum(a) from pvagg where s = x) xx;

explain (verbose, costs off)
select * from unnest(array[0, 1, 2]::int[]) x, lateral (select sum(a + x) from pvagg) xx;

select * from unnest(array[0, 1, 2]::int[]) x, lateral (select sum(a + x) from pvagg) xx;

-- The plan for this query differs after PG16, x is not used as grouping key but
-- just added into the output targetlist of partial aggregation nodes.
select * from unnest(array[0, 1, 2]::int[]) x, lateral (select sum(a) from pvagg group by x) xx;


drop table pvagg;
