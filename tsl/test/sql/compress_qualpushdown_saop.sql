-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Planning tests for compressed chunk table filter pushdown with scalar array
-- operations.
\c :TEST_DBNAME :ROLE_SUPERUSER

-- helper function: float -> pseudorandom float [-0.5..0.5]
create or replace function mix(x anyelement) returns float8 as $$
    select hashfloat8(x::float8) / pow(2, 32)
$$ language sql;

set max_parallel_workers_per_gather = 0;

create table saop(ts int, s text, id text, payload text);

select create_hypertable('saop', 'ts', chunk_time_interval => 50001);

alter table saop set (timescaledb.compress,
    timescaledb.compress_segmentby = 's',
    timescaledb.compress_orderby = 'id, ts');

insert into saop
select ts,
    ts % 23 s,
    ts % 29 id,
    (mix(ts % 1483) * 1483)::int::text payload
from generate_series(1, 100000) ts;

create index on saop(payload);

select count(compress_chunk(x)) from show_chunks('saop') x;

vacuum full analyze saop;


explain (analyze, costs off, timing off, summary off)
select * from saop where s = any(array['1', '10']);

explain (analyze, costs off, timing off, summary off)
select * from saop where id = any(array['1', '10']);

explain (analyze, costs off, timing off, summary off)
select * from saop where payload = any(array['1', '10']);

explain (analyze, costs off, timing off, summary off)
select * from saop where payload = all(array['1', '10']);

explain (analyze, costs off, timing off, summary off)
select * from saop where id < any(array['1', '10']);

explain (analyze, costs off, timing off, summary off)
select * from saop where id < all(array['1', '10']);

explain (analyze, costs off, timing off, summary off)
select * from saop where payload = any(array[s, '10']);

explain (analyze, costs off, timing off, summary off)
select * from saop where payload = any(array[s, null]);

explain (analyze, costs off, timing off, summary off)
select * from saop where payload = any(null::text[]);

explain (analyze, costs off, timing off, summary off)
select * from saop where payload = any(array[null, null]);

explain (analyze, costs off, timing off, summary off)
select * from saop where s = any(array[payload, id]);

explain (analyze, costs off, timing off, summary off)
select * from saop where s = all(array[payload, id]);
