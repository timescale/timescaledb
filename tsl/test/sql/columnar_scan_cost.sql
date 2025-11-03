-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Some primitive tests that show cost of DecompressChunk node so that we can
-- monitor the changes.

create table costtab(ts int, s text, c text, ti text, fi float);
select create_hypertable('costtab', 'ts');
alter table costtab set (timescaledb.compress, timescaledb.compress_segmentby = 's',
    timescaledb.compress_orderby = 'ts');
insert into costtab select ts, ts % 10, ts::text, ts::text, ts::float from generate_series(1, 10000) ts;
create index on costtab(ti);
create index on costtab(fi);
select count(compress_chunk(x)) from show_chunks('costtab') x;
vacuum freeze analyze costtab;


explain (buffers off) select * from costtab;

explain (buffers off) select * from costtab where s = '1';

explain (buffers off) select * from costtab where c = '100';

explain (buffers off) select * from costtab where ti = '200';

explain (buffers off) select * from costtab where fi = 200;

explain (buffers off) select ts from costtab;

explain (buffers off) select ts from costtab where s = '1';

explain (buffers off) select ts from costtab where c = '100';

explain (buffers off) select ts, s from costtab;

explain (buffers off) select ts, s from costtab where s = '1';

explain (buffers off) select ts, s from costtab where c = '100';

explain (buffers off) select * from costtab where ts = 5000;

explain (buffers off) select * from costtab where fi = 200 and ts = 5000;

explain (buffers off) select * from costtab where s = '1' or (fi = 200 and ts = 5000);


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

explain (analyze, timing off, summary off, buffers off) select * from estimate_count;


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

explain (analyze, timing off, summary off, buffers off) select * from estimate_count;


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

explain (analyze, timing off, summary off, buffers off) select * from estimate_count;


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

explain (analyze, timing off, summary off, buffers off) select * from estimate_count;


-- single row. Postgres generates all-zero entry in pg_statistics in this case,
-- but we want to avoid zero row counts.
truncate estimate_count;
insert into estimate_count
select '2025-01-01', 1, 2
;

select count(compress_chunk(c)) from show_chunks('estimate_count') c;
vacuum analyze estimate_count;

explain (analyze, timing off, summary off, buffers off) select * from estimate_count;


-- no statistics
truncate estimate_count;
vacuum analyze estimate_count;
insert into estimate_count
select t, d, 1
from generate_series('2025-01-01'::timestamptz,'2025-01-03','15 min') t,
  generate_series(1, 1000) d
;

select count(compress_chunk(c)) from show_chunks('estimate_count') c;

vacuum analyze estimate_count;

\c :TEST_DBNAME :ROLE_SUPERUSER

with hypertables as (
    select unnest(array[compressed_hypertable_id, id])
    from _timescaledb_catalog.hypertable
    where (schema_name || '.' || table_name)::regclass = 'estimate_count'::regclass)
, chunks as (
    select (schema_name || '.' || table_name)::regclass
    from _timescaledb_catalog.chunk
    where hypertable_id in (select * from hypertables)
)
delete from pg_statistic
where starelid in (select * from chunks)
;

explain (analyze, timing off, summary off, buffers off) select * from estimate_count;

-- Test a high-cardinality orderby column
create table highcard(ts int) with (tsdb.hypertable, tsdb.partition_column = 'ts',
    tsdb.compress_orderby = 'ts', tsdb.chunk_interval = 10000000);
insert into highcard select generate_series(1, 1000000);
select count(compress_chunk(x)) from show_chunks('highcard') x;
vacuum freeze analyze highcard;

explain (buffers off, analyze, timing off, summary off)
select * from highcard where ts > 200000 and ts < 300000;

explain (buffers off, analyze, timing off, summary off)
select * from highcard where ts = 500000;

explain (buffers off, analyze, timing off, summary off)
select * from highcard where ts < 500000;

explain (buffers off, analyze, timing off, summary off)
select * from highcard where ts > 500000;
