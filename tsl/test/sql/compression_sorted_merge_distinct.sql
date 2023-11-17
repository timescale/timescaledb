-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test that the compressed batch sorted merge plan is chosen based on the
-- cardinality of the segmentby columns.

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
