-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table test(ts int, tag text) with (tsdb.hypertable,
    tsdb.partition_column = 'ts', tsdb.compress_orderby = 'ts');

insert into test values (1, '1');

create index on test(tag);

select compress_chunk(x) from show_chunks('test') x;

vacuum analyze test;

-- Normal case: bloom indexes use the newest version.
explain (analyze, costs off, buffers off, timing off, summary off)
select * from test where tag = '1';


-- Create a bloom index with the old column name, should be disabled.
set timescaledb.debug_bloom1_column_prefix = 'bloom1';

select compress_chunk(decompress_chunk(x)) from show_chunks('test') x;

vacuum analyze test;

reset timescaledb.debug_bloom1_column_prefix;

explain (analyze, costs off, buffers off, timing off, summary off)
select * from test where tag = '1';


-- Try to enable it with the GUC.
set timescaledb.enable_legacy_bloom1_v1 to on;

explain (analyze, costs off, buffers off, timing off, summary off)
select * from test where tag = '1';

reset timescaledb.enable_legacy_bloom1_v1;
