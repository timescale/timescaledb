-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- helper function: float -> pseudorandom float [-0.5..0.5]
CREATE OR REPLACE FUNCTION mix(x anyelement) RETURNS float8 AS $$
    SELECT hashfloat8(x::float8) / pow(2, 32)
$$ LANGUAGE SQL;

create table test(ts int, s text, c text);
select create_hypertable('test', 'ts');
alter table test set (timescaledb.compress, timescaledb.compress_segmentby = 's',
    timescaledb.compress_orderby = 'ts');

insert into test
select ts, ts % 10 s, mix(ts)::text c from generate_series(1, 10000) ts;

insert into test
select ts, 10 + ts % 10 s, mix(ts % 100)::text c from generate_series(10001, 15000) ts;

insert into test
select ts, 20 + ts % 10 s, mix(ts % 10)::text c from generate_series(15001, 20000) ts;


create index on test using brin(c text_bloom_ops);
select count(compress_chunk(x)) from show_chunks('test') x;


-- Test the debug function
create or replace function ts_bloom1_debug_info(in _timescaledb_internal.bloom1,
    out toast_header int, out toasted_bytes int, out compressed_bytes int,
    out detoasted_bytes int, out bits_total int, out bits_set int,
    out estimated_elements int)
as :TSL_MODULE_PATHNAME, 'ts_bloom1_debug_info' language c immutable parallel safe;

select schema_name || '.' || table_name chunk, 'c' column from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'test') limit 1)
\gset

with col as (
    select _ts_meta_count rows, _ts_meta_v2_bloom1_:column f, :column cc
    from :chunk),
blooms as (
    select *, (ts_bloom1_debug_info(f)).*, pg_column_compression(f) filter_column_compression,
        pg_column_size(f) filter_column_size
    from col)
select (toast_header::bit(16), bits_total, filter_column_compression) kind,
    avg(rows)::int rows,
    count(*),
    count(*) filter (where _timescaledb_functions.bloom1_contains(f, mix(1)::text)) h,
    array_agg(distinct bits_total) filter_bit_sizes,
    array_agg(distinct filter_column_compression) filter_compression_algorithms,
    round(avg(detoasted_bytes / compressed_bytes::numeric), 2) filter_compression_ratio,
    avg(estimated_elements)::int estimated_elements,
    avg(bits_set)::int bits_set,
    round(avg(pg_column_size(cc)) / (any_value(bits_total) / 8.), 2) column_to_filter_ratio
from blooms
group by 1 order by min(bits_total);

-- One way to inspect the IO efficiency: ratio of bytes that would have been
-- read w/o the index to the sum of bytes read by the bloom filter check
-- and the actual column check.
with col as (
    select _ts_meta_count b, _ts_meta_v2_bloom1_:column f, :column cc
    from :chunk)
select
    round(
        sum(pg_column_size(cc))::numeric / (sum(pg_column_size(f))
            + sum(pg_column_size(cc)) filter (where _timescaledb_functions.bloom1_contains(f, 'match'::text))),
        2)
from col;

-- Compressed bytes-per-value vs bloom filter bytes-per-value.
with col as (
    select _ts_meta_count rows, _ts_meta_v2_bloom1_:column f, :column cc
    from :chunk)
select
    round(sum(pg_column_size(cc))::numeric / sum(rows), 2) compressed_bytes_per_row,
    round(sum(pg_column_size(f))::numeric / sum(rows), 2) filter_bytes_per_row
from col;
