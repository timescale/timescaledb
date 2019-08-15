-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0

--table with special column names --
create table foo2 (a integer, "bacB toD" integer, c integer, d integer);
select table_name from create_hypertable('foo2', 'a', chunk_time_interval=> 10);

create table non_compressed (a integer, "bacB toD" integer, c integer, d integer);
select table_name from create_hypertable('non_compressed', 'a', chunk_time_interval=> 10);
insert into non_compressed values( 3 , 16 , 20, 4);

ALTER TABLE foo2 set (timescaledb.compress, timescaledb.compress_segmentby = '"bacB toD",c' , timescaledb.compress_orderby = 'c');
ALTER TABLE foo2 set (timescaledb.compress, timescaledb.compress_segmentby = '"bacB toD",c' , timescaledb.compress_orderby = 'd');
select * from _timescaledb_catalog.hypertable_compression order by attname;

-- Negative test cases ---
--basic test with count
create table foo (a integer, b integer, c integer);
select table_name from create_hypertable('foo', 'a', chunk_time_interval=> 10);

insert into foo values( 3 , 16 , 20);
insert into foo values( 10 , 10 , 20);
insert into foo values( 20 , 11 , 20);
insert into foo values( 30 , 12 , 20);

-- should error out --
ALTER TABLE foo ALTER b SET NOT NULL, set (timescaledb.compress);

ALTER TABLE foo ALTER b SET NOT NULL;
select attname, attnotnull from pg_attribute where attrelid = (select oid from pg_class where relname like 'foo') and attname like 'b';

ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_segmentby = 'd');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'd');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c desc nulls');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c desc nulls thirsty');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c climb nulls first');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c nulls first asC');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c desc nulls first asc');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c desc hurry');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c descend');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_segmentby = 'c asc' , timescaledb.compress_orderby = 'c');

--should succeed
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'a');

select decompress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'foo' limit 1;

--should succeed
select compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'foo' limit 1;

select compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'foo' limit 1;

select compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'non_compressed' limit 1;

select decompress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'non_compressed' limit 1;
