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

-- some values for testing zero byte in the middle (not possible for text)
insert into bytea_test select 4000, 5, 0.5, '\x310031'::bytea;

insert into bytea_test select 4001, 6, 0.6, '\x31'::bytea;

select count(compress_chunk(x)) from show_chunks('bytea_test') x;

-- bytea segmentby
insert into bytea_test select ts, 4, mix(ts), (ts % 10)::text::bytea from generate_series(5001, 5999) ts;

alter table bytea_test set (tsdb.compress_segmentby = 'tag');

select count(compress_chunk(x)) from show_chunks('bytea_test') x;

vacuum analyze bytea_test;

explain (costs off, verbose, analyze, buffers off, timing off, summary off)
select * from bytea_test;


select s, count(distinct tag), min(tag::text), max(tag::text) from bytea_test
group by s order by s;


-- Filters must be vectorized.
set timescaledb.debug_require_vector_qual to 'require';
--/* uncomment to generate reference */ set timescaledb.enable_bulk_decompression to off; set timescaledb.debug_require_vector_qual to 'forbid';

select count(*) from bytea_test where ts < 5000 and tag = '\x31'::bytea;

select count(*) from bytea_test where ts < 5000 and tag != '\x31'::bytea;

select count(*) from bytea_test where ts < 5000 and tag = '\x310031'::bytea;

select count(*) from bytea_test where ts < 5000 and tag != '\x310031'::bytea;

reset timescaledb.enable_bulk_decompression;
reset timescaledb.debug_require_vector_qual;


-- Grouping must be vectorized.
set timescaledb.debug_require_vector_agg = 'require';
--/* uncomment to generate reference */ set timescaledb.enable_bulk_decompression to off; set timescaledb.debug_require_vector_agg to 'forbid';

select tag, min(value) from bytea_test group by tag order by min(value) limit 10;

reset timescaledb.enable_bulk_decompression;
reset timescaledb.debug_require_vector_agg;

