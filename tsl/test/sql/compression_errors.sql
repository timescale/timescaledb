-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0

--table with special column names --
create table foo2 (a integer, "bacB toD" integer, c integer, d integer);
select table_name from create_hypertable('foo2', 'a', chunk_time_interval=> 10);

create table foo3 (a integer, "bacB toD" integer, c integer, d integer);
select table_name from create_hypertable('foo3', 'a', chunk_time_interval=> 10);

create table non_compressed (a integer, "bacB toD" integer, c integer, d integer);
select table_name from create_hypertable('non_compressed', 'a', chunk_time_interval=> 10);
insert into non_compressed values( 3 , 16 , 20, 4);

ALTER TABLE foo2 set (timescaledb.compress_segmentby = '"bacB toD",c' , timescaledb.compress_orderby = 'c');
ALTER TABLE foo2 set (timescaledb.compress, timescaledb.compress_segmentby = '"bacB toD",c' , timescaledb.compress_orderby = 'c');
ALTER TABLE foo2 set (timescaledb.compress, timescaledb.compress_segmentby = '"bacB toD",c' , timescaledb.compress_orderby = 'd DESC');
ALTER TABLE foo2 set (timescaledb.compress, timescaledb.compress_segmentby = '"bacB toD",c' , timescaledb.compress_orderby = 'd');

create table with_rls (a integer, b integer);
ALTER TABLE with_rls ENABLE ROW LEVEL SECURITY;
select table_name from create_hypertable('with_rls', 'a', chunk_time_interval=> 10);
ALTER TABLE with_rls set (timescaledb.compress, timescaledb.compress_orderby='a');

--note that the time column "a" should be added to the end of the orderby list
select * from _timescaledb_catalog.hypertable_compression order by attname;

ALTER TABLE foo3 set (timescaledb.compress, timescaledb.compress_orderby='d DeSc NullS lAsT');
--shold allow alter since segment by was empty
ALTER TABLE foo3 set (timescaledb.compress, timescaledb.compress_orderby='d Asc NullS lAsT');
--this is ok too
ALTER TABLE foo3 set (timescaledb.compress, timescaledb.compress_segmentby = '"bacB toD",c', timescaledb.compress_orderby = 'd DeSc NullS lAsT');

-- Negative test cases ---

ALTER TABLE foo2 set (timescaledb.compress, timescaledb.compress_segmentby = '"bacB toD",c');


create table reserved_column_prefix (a integer, _ts_meta_foo integer, "bacB toD" integer, c integer, d integer);
select table_name from create_hypertable('reserved_column_prefix', 'a', chunk_time_interval=> 10);
ALTER TABLE reserved_column_prefix set (timescaledb.compress);

--basic test with count
create table foo (a integer, b integer, c integer, t text, p point);
ALTER TABLE foo ADD CONSTRAINT chk_existing CHECK(b > 0);
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
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c; SELECT 1');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = '1,2');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c + 1');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'random()');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c LIMIT 1');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'c USING <');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 't COLLATE "en_US"');

ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_segmentby = 'c asc' , timescaledb.compress_orderby = 'c');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_segmentby = 'c nulls last');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_segmentby = 'c + 1');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_segmentby = 'random()');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_segmentby = 'c LIMIT 1');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_segmentby = 'c + b');
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'a, p');

--should succeed
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'a, b');

--ddl on ht with compression
ALTER TABLE foo ADD COLUMN new_column INT;
ALTER TABLE foo DROP COLUMN a;
ALTER TABLE foo DROP COLUMN t;
ALTER TABLE foo ALTER COLUMN t SET NOT NULL;
ALTER TABLE foo RESET (timescaledb.compress);
ALTER TABLE foo ADD CONSTRAINT chk CHECK(b > 0);
ALTER TABLE foo ADD CONSTRAINT chk UNIQUE(b);
ALTER TABLE foo DROP CONSTRAINT chk_existing;

--note that the time column "a" should not be added to the end of the order by list again (should appear first)
select hc.* from _timescaledb_catalog.hypertable_compression hc inner join _timescaledb_catalog.hypertable h on (h.id = hc.hypertable_id) where h.table_name = 'foo' order by attname;

select decompress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'foo' ORDER BY ch1.id limit 1;

--test changing the segment by columns
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'a', timescaledb.compress_segmentby = 'b');

select ch1.schema_name|| '.' || ch1.table_name AS "CHUNK_NAME"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'foo' ORDER BY ch1.id limit 1 \gset


select decompress_chunk(:'CHUNK_NAME');
select decompress_chunk(:'CHUNK_NAME', if_compressed=>true);

--should succeed
select compress_chunk(:'CHUNK_NAME');
select compress_chunk(:'CHUNK_NAME');
select compress_chunk(:'CHUNK_NAME', if_not_compressed=>true);

select compress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'non_compressed' ORDER BY ch1.id limit 1;

ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'a', timescaledb.compress_segmentby = 'c');
ALTER TABLE foo set (timescaledb.compress='f');
ALTER TABLE foo reset (timescaledb.compress);

select decompress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'non_compressed' ORDER BY ch1.id limit 1;

--should succeed
select decompress_chunk(ch1.schema_name|| '.' || ch1.table_name)
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'foo' and ch1.compressed_chunk_id IS NOT NULL;

--should succeed
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'a', timescaledb.compress_segmentby = 'b');

select hc.* from _timescaledb_catalog.hypertable_compression hc inner join _timescaledb_catalog.hypertable h on (h.id = hc.hypertable_id) where h.table_name = 'foo' order by attname;

SELECT comp_hyper.schema_name|| '.' || comp_hyper.table_name as "COMPRESSED_HYPER_NAME"
FROM _timescaledb_catalog.hypertable comp_hyper
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'foo' ORDER BY comp_hyper.id LIMIT 1 \gset

select add_drop_chunks_policy(:'COMPRESSED_HYPER_NAME', INTERVAL '4 months', true);

--Constraint checking for compression
create table fortable(col integer primary key);
create table  table_constr( device_id integer,
                   timec integer ,
                   location integer ,
                   c integer constraint valid_cval check (c > 20) ,
                   d integer,
                   primary key ( device_id, timec)

);
select table_name from create_hypertable('table_constr', 'timec', chunk_time_interval=> 10);
ALTER TABLE table_constr set (timescaledb.compress, timescaledb.compress_segmentby = 'd');
alter table table_constr add constraint table_constr_uk unique (location, timec, device_id);
ALTER TABLE table_constr set (timescaledb.compress, timescaledb.compress_orderby = 'timec', timescaledb.compress_segmentby = 'device_id');
alter table table_constr add constraint table_constr_fk FOREIGN KEY(d) REFERENCES fortable(col) on delete cascade;
ALTER TABLE table_constr set (timescaledb.compress, timescaledb.compress_orderby = 'timec', timescaledb.compress_segmentby = 'device_id, location');
--exclusion constraints not allowed
alter table table_constr add constraint table_constr_exclu exclude using btree (timec with = );
ALTER TABLE table_constr set (timescaledb.compress, timescaledb.compress_orderby = 'timec', timescaledb.compress_segmentby = 'device_id, location, d');
alter table table_constr drop constraint table_constr_exclu ;
--now it works
ALTER TABLE table_constr set (timescaledb.compress, timescaledb.compress_orderby = 'timec', timescaledb.compress_segmentby = 'device_id, location, d');
--can't add fks after compression enabled
alter table table_constr add constraint table_constr_fk_add_after FOREIGN KEY(d) REFERENCES fortable(col) on delete cascade;

--FK check should not error even with dropped columns (previously had a bug related to this)
CREATE TABLE table_fk (
	time timestamptz NOT NULL,
	id1 int8 NOT NULL,
	id2 int8 NOT NULL,
	value float8 NULL,
	CONSTRAINT fk1 FOREIGN KEY (id1) REFERENCES fortable(col),
	CONSTRAINT fk2 FOREIGN KEY (id2) REFERENCES fortable(col)
);

SELECT create_hypertable('table_fk', 'time');
ALTER TABLE table_fk DROP COLUMN id1;
ALTER TABLE table_fk SET (timescaledb.compress,timescaledb.compress_segmentby = 'id2');

-- TEST fk cascade delete behavior on compressed chunk --
insert into fortable values(1);
insert into fortable values(10);
--we want 2 chunks here --
insert into table_constr values(1000, 1, 44, 44, 1);
insert into table_constr values(1000, 10, 44, 44, 10);

select ch1.schema_name|| '.' || ch1.table_name AS "CHUNK_NAME"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
where ch1.hypertable_id = ht.id and ht.table_name like 'table_constr'
ORDER BY ch1.id limit 1 \gset

-- we have 1 compressed and 1 uncompressed chunk after this.
select compress_chunk(:'CHUNK_NAME');

SELECT  hypertable_name , total_chunks , number_compressed_chunks
FROM timescaledb_information.compressed_hypertable_stats;
--delete from foreign table, should delete from hypertable too
select device_id, d from table_constr order by device_id, d;
delete from fortable where col = 1 or col = 10;
select device_id, d from table_constr order by device_id, d;

--github issue 1661
--disable compression after enabling it on a table that has fk constraints
CREATE TABLE  table_constr2( device_id integer,
                    timec integer ,
                    location integer ,
                   d integer references fortable(col),
                    primary key ( device_id, timec)
);
SELECT table_name from create_hypertable('table_constr2', 'timec', chunk_time_interval=> 10);
INSERT INTO fortable VALUES( 99 );
INSERT INTO table_constr2 VALUES( 1000, 10, 5, 99);

ALTER TABLE table_constr2 SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id');

 ALTER TABLE table_constr2 SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id, d');

--compress a chunk and try to disable compression, it should fail --
SELECT ch1.schema_name|| '.' || ch1.table_name AS "CHUNK_NAME"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id and ht.table_name like 'table_constr2' \gset
SELECT compress_chunk(:'CHUNK_NAME');
ALTER TABLE table_constr2 set (timescaledb.compress=false);

--decompress all chunks and disable compression.
SELECT decompress_chunk(:'CHUNK_NAME');
ALTER TABLE table_constr2 SET (timescaledb.compress=false);

