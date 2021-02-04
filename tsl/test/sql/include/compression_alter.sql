-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

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
WHERE hypertable.table_name like 'test1' and chunk.compressed_chunk_id IS NULL ORDER BY chunk.id
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
WHERE hypertable.table_name like 'test1' and chunk.compressed_chunk_id IS NOT NULL ORDER BY chunk.id
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
WHERE hypertable.table_name like 'test1' and chunk.compressed_chunk_id IS NULL ORDER BY chunk.id
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
WHERE hypertable.table_name = 'test1' and chunk.compressed_chunk_id IS NULL ORDER BY chunk.id
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
