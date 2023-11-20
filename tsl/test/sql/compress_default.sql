-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Use a weird compression configuration to test decompression of all-default
-- batch.
create table t(ts int);
select create_hypertable('t', 'ts');
alter table t set(timescaledb.compress, timescaledb.compress_segmentby = 'ts',
    timescaledb.compress_orderby = '');
insert into t select generate_series(1, 10000);
select count(*) from t;

select count(compress_chunk(x, true)) from show_chunks('t') x;
select count(*) from t;

select count(decompress_chunk(x, true)) from show_chunks('t') x;
select count(*) from t;


-- Now add a column which will be all default in the compressed chunk
select count(compress_chunk(x, true)) from show_chunks('t') x;

alter table t add column x int default 7;
select count(*), x from t group by x;

select count(decompress_chunk(x, true)) from show_chunks('t') x;
select count(*), x from t group by x;
