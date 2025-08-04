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


explain select * from costtab;

explain select * from costtab where s = '1';

explain select * from costtab where c = '100';

explain select * from costtab where ti = '200';

explain select * from costtab where fi = 200;

explain select ts from costtab;

explain select ts from costtab where s = '1';

explain select ts from costtab where c = '100';

explain select ts, s from costtab;

explain select ts, s from costtab where s = '1';

explain select ts, s from costtab where c = '100';

explain select * from costtab where ts = 5000;

explain select * from costtab where fi = 200 and ts = 5000;

explain select * from costtab where s = '1' or (fi = 200 and ts = 5000);


-- Test a high-cardinality orderby column
create table highcard(ts int) with (tsdb.hypertable, tsdb.partition_column = 'ts',
    tsdb.compress_orderby = 'ts', tsdb.chunk_interval = 10000000);
insert into highcard select generate_series(1, 1000000);
select count(compress_chunk(x)) from show_chunks('highcard') x;
vacuum freeze analyze highcard;

explain (analyze, timing off, summary off)
select * from highcard where ts > 200000 and ts < 300000;

explain (analyze, timing off, summary off)
select * from highcard where ts = 500000;

explain (analyze, timing off, summary off)
select * from highcard where ts < 500000;

explain (analyze, timing off, summary off)
select * from highcard where ts > 500000;
