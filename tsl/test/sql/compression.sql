-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.


--TEST1 ---
--basic test with count
create table foo (a integer, b integer, c integer);
select table_name from create_hypertable('foo', 'a', chunk_time_interval=> 10);

insert into foo values( 3 , 16 , 20);
insert into foo values( 10 , 10 , 20);
insert into foo values( 20 , 11 , 20);
insert into foo values( 30 , 12 , 20);

alter table foo set (timescaledb.compress);
select id, schema_name, table_name, compressed, compressed_hypertable_id from
_timescaledb_catalog.hypertable order by id;

-- should error out --
\set ON_ERROR_STOP 0
ALTER TABLE foo ALTER b SET NOT NULL, set (timescaledb.compress);
\set ON_ERROR_STOP 1
ALTER TABLE foo ALTER b SET NOT NULL;
select attname, attnotnull from pg_attribute where attrelid = (select oid from pg_class where relname like 'foo') and attname like 'b';
