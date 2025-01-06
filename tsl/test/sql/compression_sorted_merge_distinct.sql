-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test that the compressed batch sorted merge plan is chosen based on the
-- cardinality of the segmentby columns.

set max_parallel_workers_per_gather = 0;

-- helper function: float -> pseudorandom float [0..1].
create or replace function mix(x float4) returns float4 as $$ select ((hashfloat4(x) / (pow(2., 31) - 1) + 1) / 2)::float4 $$ language sql;

create table t(ts timestamp, low_card int, high_card int, value float);
select create_hypertable('t', 'ts');

insert into t select
    '2020-01-01'::timestamp
        + interval '1 second' * (x + 0.1 * mix(low_card + high_card + x)),
    low_card,
    high_card,
    100 * mix(low_card + high_card) * sin(x / mix(low_card + high_card + 1))
from generate_series(1, 400) x, generate_series(1, 3) low_card, generate_series(1, 700) high_card;

alter table t set (timescaledb.compress = true, timescaledb.compress_segmentby = 'low_card,high_card');
select count(compress_chunk(x, true)) from show_chunks('t') x;
analyze t;

explain (costs off) select * from t order by ts;
explain (costs off) select * from t where low_card = 1 order by ts;
explain (costs off) select * from t where high_card = 1 order by ts;
explain (costs off) select * from t where low_card = 1 and high_card = 1 order by ts;

-- Test that batch sorted merge is not disabled by enable_sort. We have another
-- GUC to disable it.
set enable_sort to off;
explain (costs off) select * from t where high_card = 1 order by ts;
reset enable_sort;

-- Test that inequality conditions in WHERE also influence the estimates.
explain (costs off) select * from t where high_card < 10 order by ts;
explain (costs off) select * from t where high_card < 500 order by ts;


-- Test that batch sorted merge respects the working memory limit.
set work_mem to 64;
explain (costs off) select * from t where high_card < 10 order by ts;
reset work_mem;

-- Test for large values of memory limit bytes that don't fit into an int.
-- Note that on i386 the max value is 2GB which is not enough to trigger the
-- overflow we had on 64-bit systems, so we have to use different values based
-- on the architecture.
select least(4194304, max_val::bigint) large_work_mem from pg_settings where name = 'work_mem' \gset
set work_mem to :large_work_mem;
explain (costs off) select * from t where high_card < 10 order by ts;
reset work_mem;

reset max_parallel_workers_per_gather;
