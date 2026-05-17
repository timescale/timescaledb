-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test for body_buffer overflow in columnar_result_set_row with
-- DT_ArrowText type. When vectorized text functions produce large
-- results, the body buffer growth computation could overflow int32.

set max_parallel_workers_per_gather to 0;

create table t_textoverflow (time timestamptz, a text);

select table_name from create_hypertable('t_textoverflow', 'time',
    chunk_time_interval => interval '1 year');

insert into t_textoverflow
select '2020-01-01'::timestamptz + i * interval '1 hour',
       repeat('x', 100000)
from generate_series(1, 10) i;

alter table t_textoverflow set (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time'
);
select count(compress_chunk(c)) from show_chunks('t_textoverflow') c;

set timescaledb.debug_require_vector_agg to 'require';
select sum(length(a || a)) from t_textoverflow;
reset timescaledb.debug_require_vector_agg;

drop table t_textoverflow cascade;

reset max_parallel_workers_per_gather;
