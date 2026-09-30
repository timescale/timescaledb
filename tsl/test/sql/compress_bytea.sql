-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- helper function: float -> pseudorandom float [-0.5..0.5]
CREATE OR REPLACE FUNCTION mix(x anyelement) RETURNS float8 AS $$
    SELECT hashfloat8(x::float8) / pow(2, 32)
$$ LANGUAGE SQL;

create table bytea_test(ts int, s int, value float) with (tsdb.hypertable,
    tsdb.partition_column = 'ts', tsdb.chunk_interval = 1000,
    tsdb.compress, tsdb.compress_orderby = 'ts');

insert into bytea_test select ts, 1, mix(ts) from generate_series(1, 999) ts;

select count(compress_chunk(x)) from show_chunks('bytea_test') x;

-- bytea with default value in previous chunk
alter table bytea_test add column tag bytea default '\x1234';

-- bytea with array compression
insert into bytea_test select ts, 2, mix(ts), ts::text::bytea from generate_series(1001, 1999) ts;

-- bytea with dictionary compression
insert into bytea_test select ts, 3, mix(ts), (ts % 10)::text::bytea from generate_series(2001, 2999) ts;

select count(compress_chunk(x)) from show_chunks('bytea_test') x;

-- bytea segmentby
insert into bytea_test select ts, 4, mix(ts), (ts % 10)::text::bytea from generate_series(3001, 3999) ts;

alter table bytea_test set (tsdb.compress_segmentby = 'tag');

select count(compress_chunk(x)) from show_chunks('bytea_test') x;

explain (costs off, verbose, analyze, buffers off, timing off, summary off)
select * from bytea_test;


select s, count(distinct tag), min(tag::text), max(tag::text) from bytea_test
group by s order by s;













