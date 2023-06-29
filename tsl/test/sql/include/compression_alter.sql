-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir compression_utils.sql
\ir ../../../../test/sql/include/test_utils.sql
CREATE TABLE test1 ("Time" timestamptz, intcol integer, bntcol bigint, txtcol text);
SELECT table_name from create_hypertable('test1', 'Time', chunk_time_interval=> INTERVAL '1 day');

INSERT INTO test1
SELECT t,  gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()::text
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-05 1:00', '1 hour') t;
INSERT INTO test1
SELECT '2018-03-04 2:00', 100, 200, 'hello' ;

ALTER TABLE test1 set (timescaledb.compress, timescaledb.compress_segmentby = 'bntcol', timescaledb.compress_orderby = '"Time" DESC');

SELECT COUNT(*) AS count_compressed
FROM
(
SELECT compress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.status & 1 = 0 ORDER BY chunk.id
)
AS sub;

-- TEST: ALTER TABLE add column tests --
ALTER TABLE test1 ADD COLUMN new_coli integer;
ALTER TABLE test1 ADD COLUMN new_colv varchar(30);

SELECT * FROM _timescaledb_catalog.hypertable_compression
ORDER BY attname;

SELECT count(*) from test1 where new_coli is not null;
SELECT count(*) from test1 where new_colv is null;

--decompress 1 chunk and query again
SELECT COUNT(*) AS count_compressed
FROM
(
SELECT decompress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.status & 1 > 0 ORDER BY chunk.id
LIMIT 1
)
AS sub;

SELECT count(*) from test1 where new_coli is not null;
SELECT count(*) from test1 where new_colv is null;

--compress all chunks and query ---
--create new chunk and fill in data --
INSERT INTO test1 SELECT t,  gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()::text , 100, '101t'
FROM generate_series('2018-03-08 1:00'::TIMESTAMPTZ, '2018-03-09 1:00', '1 hour') t;
SELECT count(*) from test1 where new_coli  = 100;
SELECT count(*) from test1 where new_colv  = '101t';

SELECT COUNT(*) AS count_compressed
FROM
(
SELECT compress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.status & 1 = 0 ORDER BY chunk.id
)
AS sub;
SELECT count(*) from test1 where new_coli  = 100;
SELECT count(*) from test1 where new_colv  = '101t';

CREATE INDEX new_index ON test1(new_colv);

-- TEST 2:  ALTER TABLE rename column
SELECT * FROM _timescaledb_catalog.hypertable_compression
WHERE attname = 'new_coli' and hypertable_id = (SELECT id from _timescaledb_catalog.hypertable
                       WHERE table_name = 'test1' );

ALTER TABLE test1 RENAME new_coli TO coli;
SELECT * FROM _timescaledb_catalog.hypertable_compression
WHERE attname = 'coli' and hypertable_id = (SELECT id from _timescaledb_catalog.hypertable
                       WHERE table_name = 'test1' );
SELECT count(*) from test1 where coli  = 100;

--rename segment by column name
ALTER TABLE test1 RENAME bntcol TO  bigintcol  ;

SELECT * FROM _timescaledb_catalog.hypertable_compression
WHERE attname = 'bigintcol' and hypertable_id = (SELECT id from _timescaledb_catalog.hypertable
                       WHERE table_name = 'test1' );

--query by segment by column name
SELECT * from test1 WHERE bigintcol = 100;
SELECT * from test1 WHERE bigintcol = 200;

-- add a new chunk and compress
INSERT INTO test1 SELECT '2019-03-04 2:00', 99, 800, 'newchunk' ;

SELECT COUNT(*) AS count_compressed
FROM
(
SELECT compress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name = 'test1' and chunk.status & 1 = 0 ORDER BY chunk.id
) q;

--check if all chunks have new column names
--both counts should be equal
SELECT count(*) FROM _timescaledb_catalog.chunk
WHERE hypertable_id =  ( SELECT id FROM _timescaledb_catalog.hypertable
                         WHERE table_name = 'test1' );

SELECT count(*)
FROM ( SELECT attrelid::regclass, attname FROM pg_attribute
       WHERE attrelid in (SELECT inhrelid::regclass from pg_inherits
                          where inhparent = 'test1'::regclass)
       and attname = 'bigintcol' ) q;

--check count on internal compression table too i.e. all the chunks have
--the correct column name
SELECT format('%I.%I', cht.schema_name, cht.table_name) AS "COMPRESSION_TBLNM"
FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.hypertable cht
WHERE ht.table_name = 'test1' and cht.id = ht.compressed_hypertable_id \gset

SELECT count(*)
FROM ( SELECT attrelid::regclass, attname FROM pg_attribute
       WHERE attrelid in (SELECT inhrelid::regclass from pg_inherits
                          where inhparent = :'COMPRESSION_TBLNM'::regclass )
       and attname = 'bigintcol' ) q;

-- check column name truncation with renames
-- check if the name change is reflected for settings
ALTER TABLE test1 RENAME  bigintcol TO
ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccabdeeeeeeccccccccccccc;

SELECT * from timescaledb_information.compression_settings
WHERE hypertable_name = 'test1' and attname like 'ccc%';

SELECT count(*)
FROM ( SELECT attrelid::regclass, attname FROM pg_attribute
       WHERE attrelid in (SELECT inhrelid::regclass from pg_inherits
                          where inhparent = :'COMPRESSION_TBLNM'::regclass )
       and attname like 'ccc%a' ) q;

ALTER TABLE test1 RENAME
ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccabdeeeeeeccccccccccccc
TO bigintcol;

SELECT * from timescaledb_information.compression_settings
WHERE hypertable_name = 'test1' and attname = 'bigintcol' ;

-- test compression default handling
CREATE TABLE test_defaults(time timestamptz NOT NULL, device_id int);
SELECT create_hypertable('test_defaults','time');
ALTER TABLE test_defaults SET (timescaledb.compress,timescaledb.compress_segmentby='device_id');

-- create 2 chunks
INSERT INTO test_defaults SELECT '2000-01-01', 1;
INSERT INTO test_defaults SELECT '2001-01-01', 1;

-- compress first chunk
SELECT compress_chunk(show_chunks) AS compressed_chunk FROM show_chunks('test_defaults') ORDER BY show_chunks::text LIMIT 1;

SELECT * FROM test_defaults ORDER BY 1;
ALTER TABLE test_defaults ADD COLUMN c1 int;
ALTER TABLE test_defaults ADD COLUMN c2 int NOT NULL DEFAULT 42;
SELECT * FROM test_defaults ORDER BY 1,2;

-- try insert into compressed and recompress
INSERT INTO test_defaults SELECT '2000-01-01', 2;
SELECT * FROM test_defaults ORDER BY 1,2;
CALL recompress_all_chunks('test_defaults', 1, false);
SELECT * FROM test_defaults ORDER BY 1,2;

-- timescale/timescaledb#5412
ALTER TABLE test_defaults ADD COLUMN c3 int NOT NULL DEFAULT 43;
SELECT *,assert_equal(c3,43) FROM test_defaults ORDER BY 1,2;
select decompress_chunk(show_chunks('test_defaults'),true);
SELECT *,assert_equal(c3,43) FROM test_defaults ORDER BY 1,2;
select compress_chunk(show_chunks('test_defaults'));
SELECT *,assert_equal(c3,43) FROM test_defaults ORDER BY 1,2;

-- test dropping columns from compressed
CREATE TABLE test_drop(f1 text, f2 text, f3 text, time timestamptz, device int, o1 text, o2 text);
SELECT create_hypertable('test_drop','time');
ALTER TABLE test_drop SET (timescaledb.compress,timescaledb.compress_segmentby='device',timescaledb.compress_orderby='o1,o2');

-- dropping segmentby or orderby columns will fail
\set ON_ERROR_STOP 0
ALTER TABLE test_drop DROP COLUMN time;
ALTER TABLE test_drop DROP COLUMN o1;
ALTER TABLE test_drop DROP COLUMN o2;
ALTER TABLE test_drop DROP COLUMN device;
\set ON_ERROR_STOP 1

-- switch to WARNING only to suppress compress_chunk NOTICEs
SET client_min_messages TO WARNING;

-- create some chunks each with different physical layout
ALTER TABLE test_drop DROP COLUMN f1;
INSERT INTO test_drop SELECT NULL,NULL,'2000-01-01',1,'o1','o2';
SELECT count(compress_chunk(chunk,true)) FROM show_chunks('test_drop') chunk;

ALTER TABLE test_drop DROP COLUMN f2;
-- test non-existant column
\set ON_ERROR_STOP 0
ALTER TABLE test_drop DROP COLUMN f10;
\set ON_ERROR_STOP 1
ALTER TABLE test_drop DROP COLUMN IF EXISTS f10;

INSERT INTO test_drop SELECT NULL,'2001-01-01',2,'o1','o2';
SELECT count(compress_chunk(chunk,true)) FROM show_chunks('test_drop') chunk;
ALTER TABLE test_drop DROP COLUMN f3;
INSERT INTO test_drop SELECT '2003-01-01',3,'o1','o2';
SELECT count(compress_chunk(chunk,true)) FROM show_chunks('test_drop') chunk;
ALTER TABLE test_drop ADD COLUMN c1 TEXT;
ALTER TABLE test_drop ADD COLUMN c2 TEXT;
INSERT INTO test_drop SELECT '2004-01-01',4,'o1','o2','c1','c2-4';
SELECT count(compress_chunk(chunk,true)) FROM show_chunks('test_drop') chunk;
ALTER TABLE test_drop DROP COLUMN c1;
INSERT INTO test_drop SELECT '2005-01-01',5,'o1','o2','c2-5';
SELECT count(compress_chunk(chunk,true)) FROM show_chunks('test_drop') chunk;

RESET client_min_messages;
SELECT * FROM test_drop ORDER BY 1;

-- check dropped columns got removed from catalog
-- only c2 should be left in metadata
SELECT attname
FROM _timescaledb_catalog.hypertable_compression htc
INNER JOIN _timescaledb_catalog.hypertable ht
  ON ht.id=htc.hypertable_id AND ht.table_name='test_drop'
WHERE attname NOT IN ('time','device','o1','o2')
ORDER BY 1;

