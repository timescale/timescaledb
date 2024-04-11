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


explain (costs off)
select * from unnest(array[0, 1, 2]::int[]) x, lateral (select sum(a) from pvagg where s = x) xx;

select * from unnest(array[0, 1, 2]::int[]) x, lateral (select sum(a) from pvagg where s = x) xx;


drop table pvagg;
