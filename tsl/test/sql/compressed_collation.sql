-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- We have different collation names such as en_US, en-US-x-icu and so on,
-- that are available on different platforms.
select * from (
    select 3 priority, 'C' "COLLATION"
    union all (select 2, collname from pg_collation where collname ilike 'en_us%' order by collencoding, collname limit 1)
    union all (select 1, collname from pg_collation where collname ilike 'en_us_utf%8%' order by collencoding, collname limit 1)
) c
order by priority limit 1 \gset

create table compressed_collation_ht(time timestamp, name text collate :"COLLATION",
    value float);

select create_hypertable('compressed_collation_ht', 'time');

alter table compressed_collation_ht set (timescaledb.compress,
    timescaledb.compress_segmentby = 'name', timescaledb.compress_orderby = 'time');

insert into compressed_collation_ht values ('2021-01-01 01:01:01', 'á', '1'),
    ('2021-01-01 01:01:02', 'b', '2'), ('2021-01-01 01:01:03', 'ç', '2');

SELECT count(compress_chunk(ch)) FROM show_chunks('compressed_collation_ht') ch;

vacuum analyze compressed_collation_ht;

SELECT format('%I.%I',ch.schema_name, ch.table_name) AS "CHUNK"
FROM _timescaledb_catalog.hypertable ht
    INNER JOIN _timescaledb_catalog.chunk ch
    ON ch.hypertable_id = ht.compressed_hypertable_id
        AND ht.table_name = 'compressed_collation_ht' \gset

create index on :CHUNK (name);

set enable_seqscan to off;

explain (costs off)
select * from compressed_collation_ht order by name;

select * from compressed_collation_ht order by name;
