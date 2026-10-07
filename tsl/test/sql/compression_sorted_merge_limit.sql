-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Batch sorted merge with ORDER BY ... LIMIT over many segments.

create table sm_limit(ts timestamptz not null, id int, val float8);
select create_hypertable('sm_limit', 'ts', chunk_time_interval => interval '1 year');
alter table sm_limit set (timescaledb.compress,
    timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'ts');

-- 1000 segments that all start at the same time, 10 rows each
insert into sm_limit
select '2024-01-01'::timestamptz + t * interval '1 minute', id, id + t
from generate_series(1, 1000) id, generate_series(0, 9) t;

select count(compress_chunk(x)) from show_chunks('sm_limit') x;
vacuum analyze sm_limit;

set max_parallel_workers_per_gather = 0;
set timescaledb.enable_deferred_chunk_append = off;

-- Output stays in order when many batches start at the same value.
-- The per-value counts show that each value is complete before the next.
set timescaledb.debug_require_batch_sorted_merge = 'force';

select ts, count(*) from (select ts from sm_limit order by ts limit 2500) s
group by ts order by ts;

select ts, count(*) from (select ts from sm_limit order by ts desc limit 2500) s
group by ts order by ts desc;

-- The first row of every batch is filtered out
select ts, count(*) from (select ts from sm_limit
    where ts > '2024-01-01' order by ts limit 1500) s
group by ts order by ts;

-- Every batch emits rows in order, and none is lost
select count(*) filter (where ts < prev) out_of_order, count(*) total
from (select ts, lag(ts) over () prev from (select ts from sm_limit order by ts) s) s2;

select count(*) filter (where ts > prev) out_of_order, count(*) total
from (select ts, lag(ts) over () prev from (select ts from sm_limit order by ts desc) s) s2;

-- Segments that start later, mixed with the tied ones
insert into sm_limit
select '2024-01-01'::timestamptz + interval '30 seconds' + t * interval '1 minute', id, id + t
from generate_series(1001, 1100) id, generate_series(0, 9) t;
select count(compress_chunk(x)) from show_chunks('sm_limit') x;
vacuum analyze sm_limit;

select ts, count(*) from (select ts from sm_limit order by ts limit 2200) s
group by ts order by ts;

select count(*) filter (where ts < prev) out_of_order, count(*) total
from (select ts, lag(ts) over () prev from (select ts from sm_limit order by ts) s) s2;

reset timescaledb.debug_require_batch_sorted_merge;

-- With a LIMIT, the merge only has to keep about LIMIT batches open, so a
-- small work_mem is enough for a small LIMIT.
set work_mem = '64kB';

-- small LIMIT: batch sorted merge
explain (costs off) select * from sm_limit order by ts limit 10;
explain (costs off) select * from sm_limit order by ts desc limit 10;

-- a filter means more batches per output row, but still few
explain (costs off) select * from sm_limit where val > 500 order by ts limit 10;

-- LIMIT close to the number of segments, or no LIMIT: plain sort
explain (costs off) select * from sm_limit order by ts limit 1000;
explain (costs off) select * from sm_limit order by ts;

-- The LIMIT applies to the join result, not to the scan
create table sm_limit_dim(id int primary key);
insert into sm_limit_dim select generate_series(1, 1100, 10);
analyze sm_limit_dim;
explain (costs off) select * from sm_limit m join sm_limit_dim d using (id)
order by m.ts limit 10;

reset work_mem;
drop table sm_limit_dim;
drop table sm_limit;
