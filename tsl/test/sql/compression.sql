-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.


--TEST1 ---
--basic test with count
create table foo (a integer, b integer, c integer, d integer);
select table_name from create_hypertable('foo', 'a', chunk_time_interval=> 10);

insert into foo values( 3 , 16 , 20, 11);
insert into foo values( 10 , 10 , 20, 120);
insert into foo values( 20 , 11 , 20, 13);
insert into foo values( 30 , 12 , 20, 14);

alter table foo set (timescaledb.compress, timescaledb.compress_segmentby = 'a,b', timescaledb.compress_orderby = 'c desc, d asc nulls last');
select id, schema_name, table_name, compressed, compressed_hypertable_id from
_timescaledb_catalog.hypertable order by id;
select * from _timescaledb_catalog.hypertable_compression order by hypertable_id, attname;

-- TEST2 compress-chunk for the chunks created earlier --
select compress_chunk( '_timescaledb_internal._hyper_1_2_chunk');

select compress_chunk( '_timescaledb_internal._hyper_1_1_chunk');
\x
select * from _timescaledb_catalog.compression_chunk_size
order by chunk_id;
\x
select  ch1.id, ch1.schema_name, ch1.table_name ,  ch2.table_name as compress_table
from
_timescaledb_catalog.chunk ch1, _timescaledb_catalog.chunk ch2
where ch1.compressed_chunk_id = ch2.id;

\set ON_ERROR_STOP 0
--cannot recompress the chunk the second time around
select compress_chunk( '_timescaledb_internal._hyper_1_2_chunk');

