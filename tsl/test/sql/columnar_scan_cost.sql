-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Some primitive tests that show cost of DecompressChunk node so that we can
-- monitor the changes.

create table t(ts int, s text, c text);
select create_hypertable('t', 'ts');
alter table t set (timescaledb.compress, timescaledb.compress_segmentby = 's',
    timescaledb.compress_orderby = 'ts');
insert into t select ts, ts % 10, ts::text from generate_series(1, 10000) ts;
select count(compress_chunk(x)) from show_chunks('t') x;
vacuum freeze analyze t;


explain select * from t;

explain select * from t where s = '1';

explain select * from t where c = '100';

explain select ts from t;

explain select ts from t where s = '1';

explain select ts from t where c = '100';

explain select ts, s from t;

explain select ts, s from t where s = '1';

explain select ts, s from t where c = '100';


-- Test estimation of compressed batch size using the _ts_meta_count stats.
create table estimate_count(time timestamptz, device int, value float);
select create_hypertable('estimate_count','time');
alter table estimate_count
  set (timescaledb.compress,
       timescaledb.compress_segmentby = 'device',
       timescaledb.compress_orderby   = 'time');

-- same batch sizes
insert into estimate_count
select t, d, 1
from generate_series('2025-01-01'::timestamptz,'2025-01-03','15 min') t,
  generate_series(1, 1000) d
;

select count(compress_chunk(c)) from show_chunks('estimate_count') c;
vacuum analyze estimate_count;

explain (analyze, timing off, summary off) select * from estimate_count;


-- different batch sizes
truncate estimate_count;
insert into estimate_count
select t, d, 2
from generate_series(1, 1000) d,
    lateral generate_series('2025-01-01'::timestamptz,'2025-01-03',
        interval '15 min' * (d % 10 + 1)) t
;

select count(compress_chunk(c)) from show_chunks('estimate_count') c;
vacuum analyze estimate_count;

explain (analyze, timing off, summary off) select * from estimate_count;


-- more different batch sizes
truncate estimate_count;
insert into estimate_count
select t, d, 2
from generate_series(1, 1000) d,
    lateral generate_series('2025-01-01'::timestamptz,'2025-01-03',
        interval '15 min' + interval '1 minute' * d) t
;

select count(compress_chunk(c)) from show_chunks('estimate_count') c;
vacuum analyze estimate_count;

explain (analyze, timing off, summary off) select * from estimate_count;


-- more different + one very frequent
truncate estimate_count;
insert into estimate_count
select t, d, 2
from generate_series(1, 1000) d,
    lateral generate_series('2025-01-01'::timestamptz,'2025-01-03',
        case when d % 2 = 0 then interval '10 min'
            else interval '15 min' + interval '1 minute' * d end) t
;

select count(compress_chunk(c)) from show_chunks('estimate_count') c;
vacuum analyze estimate_count;

explain (analyze, timing off, summary off) select * from estimate_count;

-- no statistics
insert into estimate_count
select t, d, 1
from generate_series('2025-01-01'::timestamptz,'2025-01-03','15 min') t,
  generate_series(1, 1000) d
;

select count(compress_chunk(c)) from show_chunks('estimate_count') c;

explain (analyze, timing off, summary off) select * from estimate_count;
