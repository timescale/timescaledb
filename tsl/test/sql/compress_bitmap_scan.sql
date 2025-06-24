-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Exercise the Bitmap Heap Scan over the compressed chunk table. It requires
-- big tables with high selectivity OR queries on indexed columns, so it almost
-- doesn't happen in the normal tests.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- helper function: float -> pseudorandom float [-0.5..0.5]
create or replace function mix(x anyelement) returns float8 as $$
    select hashfloat8(x::float8) / pow(2, 32)
$$ language sql;

set max_parallel_workers_per_gather = 0;
set enable_memoize to off;

create table bscan(ts int, s int, id int, payload int);

select create_hypertable('bscan', 'ts', chunk_time_interval => 500001);

alter table bscan set (timescaledb.compress,
    timescaledb.compress_segmentby = 's',
    timescaledb.compress_orderby = 'id, ts');

insert into bscan
select ts,
    ts % 239 s,
    ts % 111721 id,
    (mix(ts % 1483) * 1483)::int payload
from generate_series(1, 1000000) ts;

create index on bscan(payload);

select count(compress_chunk(x)) from show_chunks('bscan') x;

vacuum full analyze bscan;


-- We have many conditions here, so it's less selective and the bitmap scan
-- overhead grows. This query should use Seq Scan.
explain (analyze, verbose, costs off, timing off, summary off)
select * from bscan where id = 1 or id = 2 or id = 3 or id = 4 or id = 5
    or id = 6 or id = 7 or id = 8
;

-- This should be Bitmap Heap Scan because we have an OR of highly selective
-- conditions.
explain (analyze, verbose, costs off, timing off, summary off)
select * from bscan where id = 1 or id = 2
;

-- Also try a join with a Bitmap Heap Scan
explain (analyze, verbose, costs off, timing off, summary off)
select * from bscan t1, bscan t2
where (t2.id = 1 or t2.id = 2)
    and t1.s = t2.s
    and t1.payload = -537;
;
