-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

create table bloom(ts int, value text);
select create_hypertable('bloom', 'ts');
insert into bloom select x, md5(x::text) from generate_series(1, 10000) x;
create index on bloom(value);
alter table bloom set (timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'ts');
select count(compress_chunk(x)) from show_chunks('bloom') x;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'bloom') limit 1)
\gset

\d+ :chunk

--\set VERBOSITY verbose
--select * from :chunk;

--CREATE OR REPLACE FUNCTION ts_bloom1_matches(anyelement, bytea) RETURNS bloom
--AS :TSL_MODULE_PATHNAME, 'ts_bloom1_matches'
--LANGUAGE C IMMUTABLE STRICT;

explain (analyze, verbose, costs off, timing off, summary off)
select count(*) from bloom where value = md5(7248::text);

select count(*) from bloom where value = md5(7248::text);
