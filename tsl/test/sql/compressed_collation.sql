-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- We have different collation names such as en_US, en-US-x-icu and so on,
-- that are available on different platforms.
select * from (
    select 3 priority, 'en_US' "COLLATION"
    union all (select 2, collname from pg_collation where collname ilike 'en_us%' order by collname limit 1)
    union all (select 1, collname from pg_collation where collname ilike 'en_us_utf%8%' order by collname limit 1)
) c
order by priority limit 1 \gset

create table compressed_collation_ht(time timestamp, name text collate :"COLLATION",
    value float);

select create_hypertable('compressed_collation_ht', 'time');

alter table compressed_collation_ht set (timescaledb.compress,
    timescaledb.compress_segmentby = 'name', timescaledb.compress_orderby = 'time');

insert into compressed_collation_ht values ('2021-01-01 01:01:01', 'á', '1'),
    ('2021-01-01 01:01:02', 'b', '2'), ('2021-01-01 01:01:03', 'ç', '2');

select 1 from (
	select compress_chunk(chunk_schema || '.' || chunk_name)
	from timescaledb_information.chunks
	where hypertable_name = 'compressed_collation_ht'
) t;

select ht.schema_name || '.' || ht.table_name as "CHUNK"
from _timescaledb_catalog.hypertable ht
    inner join _timescaledb_catalog.hypertable ht2
    on ht.id = ht2.compressed_hypertable_id
        and ht2.table_name = 'compressed_collation_ht' \gset

create index on :CHUNK (name);

set enable_seqscan to off;

explain (costs off)
select * from compressed_collation_ht order by name;

select * from compressed_collation_ht order by name;
