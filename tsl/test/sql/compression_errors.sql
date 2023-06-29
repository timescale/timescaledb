-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0
\set VERBOSITY default

\set ECHO none
\o /dev/null
\ir ../../../test/sql/include/test_utils.sql
\o
\set ECHO all

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

-- this is acceptable: having previously set the default value for orderby
-- and skipping orderby on a subsequent alter command
create table default_skipped (a integer not null, b integer, c integer, d integer);
select create_hypertable('default_skipped', 'a', chunk_time_interval=> 10);
alter table default_skipped set (timescaledb.compress, timescaledb.compress_segmentby = 'c');
alter table default_skipped set (timescaledb.compress, timescaledb.compress_segmentby = 'c');

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

alter table default_skipped set (timescaledb.compress, timescaledb.compress_orderby = 'a asc', timescaledb.compress_segmentby = 'c');
alter table default_skipped set (timescaledb.compress, timescaledb.compress_segmentby = 'c');

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
ALTER TABLE foo DROP COLUMN a;
ALTER TABLE foo DROP COLUMN b;
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
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht where ch1.hypertable_id = ht.id and ht.table_name like 'foo' and ch1.status & 1 > 0;

--should succeed
ALTER TABLE foo set (timescaledb.compress, timescaledb.compress_orderby = 'a', timescaledb.compress_segmentby = 'b');

select hc.* from _timescaledb_catalog.hypertable_compression hc inner join _timescaledb_catalog.hypertable h on (h.id = hc.hypertable_id) where h.table_name = 'foo' order by attname;

SELECT comp_hyper.schema_name|| '.' || comp_hyper.table_name as "COMPRESSED_HYPER_NAME"
FROM _timescaledb_catalog.hypertable comp_hyper
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'foo' ORDER BY comp_hyper.id LIMIT 1 \gset

select add_retention_policy(:'COMPRESSED_HYPER_NAME', INTERVAL '4 months', true);

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
BEGIN;
ALTER TABLE table_constr set (timescaledb.compress, timescaledb.compress_segmentby = 'd');
ROLLBACK;
alter table table_constr add constraint table_constr_uk unique (location, timec, device_id);
BEGIN;
ALTER TABLE table_constr set (timescaledb.compress, timescaledb.compress_orderby = 'timec', timescaledb.compress_segmentby = 'device_id');
ROLLBACK;

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

-- ddl ADD column variants that are not supported
ALTER TABLE table_constr ADD COLUMN newcol integer CHECK ( newcol < 10 );
ALTER TABLE table_constr ADD COLUMN newcol integer UNIQUE;
ALTER TABLE table_constr ADD COLUMN newcol integer PRIMARY KEY;
ALTER TABLE table_constr ADD COLUMN newcol integer NOT NULL;
ALTER TABLE table_constr ADD COLUMN newcol integer DEFAULT random() + random();
ALTER TABLE table_constr ADD COLUMN IF NOT EXISTS newcol integer REFERENCES fortable(col);
ALTER TABLE table_constr ADD COLUMN IF NOT EXISTS newcol integer GENERATED ALWAYS AS IDENTITY;
ALTER TABLE table_constr ADD COLUMN IF NOT EXISTS newcol integer GENERATED BY DEFAULT AS IDENTITY;
ALTER TABLE table_constr ADD COLUMN newcol nonexistent_type;

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

SELECT  total_chunks , number_compressed_chunks
FROM hypertable_compression_stats('table_constr');

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

-- TEST compression policy
-- modify the config to trigger errors at runtime
CREATE TABLE test_table_int(time bigint, val int);
SELECT create_hypertable('test_table_int', 'time', chunk_time_interval => 1);

CREATE OR REPLACE function dummy_now() returns BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::BIGINT';
SELECT set_integer_now_func('test_table_int', 'dummy_now');
INSERT INTO test_table_int SELECT generate_series(1,5), 10;
ALTER TABLE test_table_int set (timescaledb.compress);
SELECT add_compression_policy('test_table_int', 2::int) AS compressjob_id
\gset

\c :TEST_DBNAME :ROLE_SUPERUSER
UPDATE _timescaledb_config.bgw_job
SET config = config - 'compress_after'
WHERE id = :compressjob_id;
SELECT config FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;
--should fail
CALL run_job(:compressjob_id);

SELECT remove_compression_policy('test_table_int');

--again add a new policy that we'll tamper with
SELECT add_compression_policy('test_table_int', 2::int) AS compressjob_id
\gset
UPDATE _timescaledb_config.bgw_job
SET config = config - 'hypertable_id'
WHERE id = :compressjob_id;
SELECT config FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

--should fail
CALL run_job(:compressjob_id);

UPDATE _timescaledb_config.bgw_job
SET config = NULL
WHERE id = :compressjob_id;
SELECT config FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

--should fail
CALL run_job(:compressjob_id);

-- test ADD COLUMN IF NOT EXISTS
CREATE TABLE metric (time TIMESTAMPTZ NOT NULL, val FLOAT8 NOT NULL, dev_id INT4 NOT NULL);
SELECT create_hypertable('metric', 'time', 'dev_id', 10);
ALTER TABLE metric SET (
timescaledb.compress,
timescaledb.compress_segmentby = 'dev_id',
timescaledb.compress_orderby = 'time DESC'
);

INSERT INTO metric(time, val, dev_id)
SELECT s.*, 3.14+1, 1
FROM generate_series('2021-08-17 00:00:00'::timestamp,
                     '2021-08-17 00:02:00'::timestamp, '1 s'::interval) s;
SELECT compress_chunk(show_chunks('metric'));
-- column does not exist the first time
ALTER TABLE metric ADD COLUMN IF NOT EXISTS "medium" VARCHAR ;
-- column already exists the second time
ALTER TABLE metric ADD COLUMN IF NOT EXISTS "medium" VARCHAR ;
-- also add one without IF NOT EXISTS
ALTER TABLE metric ADD COLUMN "medium_1" VARCHAR ;
ALTER TABLE metric ADD COLUMN "medium_1" VARCHAR ;

--github issue 3481
--GROUP BY error when setting compress_segmentby with an enum column

CREATE TYPE an_enum_type AS ENUM ('home', 'school');

CREATE TABLE test (
	time timestamp NOT NULL,
	enum_col an_enum_type NOT NULL
);

SELECT create_hypertable(
    'test', 'time'
);
INSERT INTO test VALUES ('2001-01-01 00:00', 'home'),
                        ('2001-01-01 01:00', 'school'),
                        ('2001-01-01 02:00', 'home');

--enable compression on enum_col
ALTER TABLE test SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'enum_col',
	timescaledb.compress_orderby = 'time'
);

--below queries will pass before chunks are compressed
SELECT 1 FROM test GROUP BY enum_col;
EXPLAIN SELECT DISTINCT 1 FROM test;

--compress chunks
SELECT COMPRESS_CHUNK(X) FROM SHOW_CHUNKS('test') X;
ANALYZE test;

--below query should pass after chunks are compressed
SELECT 1 FROM test GROUP BY enum_col;
EXPLAIN SELECT DISTINCT 1 FROM test;

--github issue 4398
SELECT format('CREATE TABLE data_table AS SELECT now() AS tm, %s', array_to_string(array_agg(format('125 AS c%s',a)), ', ')) FROM generate_series(1,550)a \gexec
CREATE TABLE ts_table (LIKE data_table);
SELECT * FROM create_hypertable('ts_table', 'tm');
--should report a warning
\set VERBOSITY terse
ALTER TABLE ts_table SET(timescaledb.compress, timescaledb.compress_segmentby = 'c1',
					   timescaledb.compress_orderby = 'tm');
INSERT INTO ts_table SELECT * FROM data_table;
--cleanup tables
DROP TABLE data_table cascade;
DROP TABLE ts_table cascade;

-- #5458 invalid reads for row expressions after column dropped on compressed tables
CREATE TABLE readings(
    "time"  TIMESTAMPTZ NOT NULL,
    battery_status  TEXT,
    battery_temperature  DOUBLE PRECISION
);

INSERT INTO readings ("time") VALUES ('2022-11-11 11:11:11-00');

SELECT create_hypertable('readings', 'time', chunk_time_interval => interval '12 hour', migrate_data=>true);

create unique index readings_uniq_idx on readings("time",battery_temperature);
ALTER TABLE readings SET (timescaledb.compress,timescaledb.compress_segmentby = 'battery_temperature');
SELECT compress_chunk(show_chunks('readings'));

ALTER TABLE readings DROP COLUMN battery_status;
INSERT INTO readings ("time", battery_temperature) VALUES ('2022-11-11 11:11:11', 0.2);
SELECT readings FROM readings;

-- #5577 On-insert decompression after schema changes may not work properly

SELECT decompress_chunk(show_chunks('readings'),true);
SELECT compress_chunk(show_chunks('readings'),true);

\set ON_ERROR_STOP 0
INSERT INTO readings ("time", battery_temperature) VALUES
    ('2022-11-11 11:11:11',0.2) -- same record as inserted
;

\set ON_ERROR_STOP 1

SELECT * from readings;
SELECT assert_equal(count(1), 2::bigint) FROM readings;

-- no unique check failure during decompression
SELECT decompress_chunk(show_chunks('readings'),true);

-- #5553 Unique constraints are not always respected on compressed tables
CREATE TABLE main_table AS
SELECT '2011-11-11 11:11:11'::timestamptz AS time, 'foo' AS device_id;

CREATE UNIQUE INDEX xm ON main_table(time, device_id);

SELECT create_hypertable('main_table', 'time', chunk_time_interval => interval '12 hour', migrate_data => TRUE);

ALTER TABLE main_table SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id',
    timescaledb.compress_orderby = '');

SELECT compress_chunk(show_chunks('main_table'));

-- insert rejected
\set ON_ERROR_STOP 0
INSERT INTO main_table VALUES
    ('2011-11-11 11:11:11', 'foo');

-- insert rejected in case 1st row doesn't violate constraint with different segmentby
INSERT INTO main_table VALUES
    ('2011-11-11 11:12:11', 'bar'),
    ('2011-11-11 11:11:11', 'foo');

\set ON_ERROR_STOP 1
SELECT assert_equal(count(1), 1::bigint) FROM main_table;

-- no unique check failure during decompression
SELECT decompress_chunk(show_chunks('main_table'), TRUE);


DROP TABLE IF EXISTS readings;

CREATE TABLE readings(
    "time" timestamptz NOT NULL,
    battery_status text,
    candy integer,
    battery_status2 text,
    battery_temperature text
);

SELECT create_hypertable('readings', 'time', chunk_time_interval => interval '12 hour');

CREATE UNIQUE INDEX readings_uniq_idx ON readings("time", battery_temperature);

ALTER TABLE readings SET (timescaledb.compress, timescaledb.compress_segmentby = 'battery_temperature');

ALTER TABLE readings DROP COLUMN battery_status;
ALTER TABLE readings DROP COLUMN battery_status2;

INSERT INTO readings("time", candy, battery_temperature)
    VALUES ('2022-11-11 11:11:11', 88, '0.2');

SELECT compress_chunk(show_chunks('readings'), TRUE);

-- no error happens
INSERT INTO readings("time", candy, battery_temperature)
    VALUES ('2022-11-11 11:11:11', 33, 0.3)
;

-- Segmentby checks should be done for unique indexes without
-- constraints, so create a table without constraints and add a unique
-- index and try to create a table without using the right segmentby
-- column.
CREATE TABLE table_unique_index(
       location smallint not null,
       device_id smallint not null,
       time timestamptz not null,
       value float8 not null
);
CREATE UNIQUE index ON table_unique_index(location, device_id, time);
SELECT table_name FROM create_hypertable('table_unique_index', 'time');
-- Will warn because the lack of segmentby/orderby compression options
ALTER TABLE table_unique_index SET (timescaledb.compress);
ALTER TABLE table_unique_index SET (timescaledb.compress = off);
ALTER TABLE table_unique_index SET (timescaledb.compress, timescaledb.compress_segmentby = 'location');
ALTER TABLE table_unique_index SET (timescaledb.compress = off);
ALTER TABLE table_unique_index SET (timescaledb.compress, timescaledb.compress_orderby = 'device_id');
ALTER TABLE table_unique_index SET (timescaledb.compress = off);
-- Will enable compression without warnings
ALTER TABLE table_unique_index SET (timescaledb.compress, timescaledb.compress_segmentby = 'location', timescaledb.compress_orderby = 'device_id');
ALTER TABLE table_unique_index SET (timescaledb.compress = off);
ALTER TABLE table_unique_index SET (timescaledb.compress, timescaledb.compress_segmentby = 'device_id,location');
ALTER TABLE table_unique_index SET (timescaledb.compress = off);
ALTER TABLE table_unique_index SET (timescaledb.compress, timescaledb.compress_orderby = 'device_id,location');
ALTER TABLE table_unique_index SET (timescaledb.compress = off);
ALTER TABLE table_unique_index SET (timescaledb.compress, timescaledb.compress_segmentby = 'time,location', timescaledb.compress_orderby = 'device_id');
ALTER TABLE table_unique_index SET (timescaledb.compress = off);
ALTER TABLE table_unique_index SET (timescaledb.compress, timescaledb.compress_segmentby = 'time,location,device_id');
ALTER TABLE table_unique_index SET (timescaledb.compress = off);

