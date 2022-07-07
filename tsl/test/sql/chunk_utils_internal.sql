-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--  These tests work for PG14 or greater
-- Remember to corordinate any changes to functionality with the Cloud
-- Storage team. Tests for the following API:
-- * freeze_chunk
-- * drop_chunk
-- * attach_foreign_table_chunk

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA test1;
GRANT CREATE ON SCHEMA test1 TO :ROLE_DEFAULT_PERM_USER;
GRANT USAGE ON SCHEMA test1 TO :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_DEFAULT_PERM_USER;
CREATE TABLE test1.hyper1 (time bigint, temp float);

SELECT create_hypertable('test1.hyper1', 'time', chunk_time_interval => 10);

INSERT INTO test1.hyper1 VALUES (10, 0.5);

INSERT INTO test1.hyper1 VALUES (30, 0.5);
SELECT chunk_schema as "CHSCHEMA",  chunk_name as "CHNAME", 
       range_start_integer, range_end_integer
FROM timescaledb_information.chunks
WHERE hypertable_name = 'hyper1' and hypertable_schema = 'test1'
ORDER BY chunk_name ;

----- TESTS for freeze and unfreeze chunk ------------
--TEST internal api that freezes a chunk
--freeze one of the chunks
SELECT chunk_schema || '.' ||  chunk_name as "CHNAME", chunk_name as "CHUNK_NAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'hyper1' and hypertable_schema = 'test1'
ORDER BY chunk_name LIMIT 1
\gset

SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME');

SELECT * from test1.hyper1 ORDER BY 1;

-- TEST updates and deletes on frozen chunk should fail
\set ON_ERROR_STOP 0
UPDATE test1.hyper1 SET temp = 40 WHERE time = 20;
UPDATE test1.hyper1 SET temp = 40 WHERE temp = 0.5;
SELECT * from test1.hyper1 ORDER BY 1;

DELETE FROM test1.hyper1 WHERE time = 20;
DELETE FROM test1.hyper1 WHERE temp = 0.5;
SELECT * from test1.hyper1 ORDER BY 1;

-- TEST inserts into a frozen chunk fails
INSERT INTO test1.hyper1 VALUES ( 11, 11);
\set ON_ERROR_STOP 1

--insert into non-frozen chunk works
INSERT INTO test1.hyper1 VALUES ( 31, 31);
SELECT * from test1.hyper1 ORDER BY 1;

-- TEST unfreeze frozen chunk and then drop
SELECT table_name, status 
FROM _timescaledb_catalog.chunk WHERE table_name = :'CHUNK_NAME';

SELECT  _timescaledb_internal.unfreeze_chunk( :'CHNAME');

--verify status in catalog
SELECT table_name, status 
FROM _timescaledb_catalog.chunk WHERE table_name = :'CHUNK_NAME';
--unfreezing again works
SELECT  _timescaledb_internal.unfreeze_chunk( :'CHNAME');
SELECT  _timescaledb_internal.drop_chunk( :'CHNAME');

-- TEST freeze_chunk api on a chunk that is compressed
CREATE TABLE public.table_to_compress (time date NOT NULL, acq_id bigint, value bigint);
CREATE INDEX idx_table_to_compress_acq_id ON public.table_to_compress(acq_id);
SELECT create_hypertable('public.table_to_compress', 'time', chunk_time_interval => interval '1 day');
ALTER TABLE public.table_to_compress SET (timescaledb.compress, timescaledb.compress_segmentby = 'acq_id');

INSERT INTO public.table_to_compress VALUES ('2020-01-01', 1234567, 777888);
INSERT INTO public.table_to_compress VALUES ('2020-02-01', 567567, 890890);
INSERT INTO public.table_to_compress VALUES ('2020-02-10', 1234, 5678);

SELECT show_chunks('public.table_to_compress');

SELECT chunk_schema || '.' ||  chunk_name as "CHNAME", chunk_name as "CHUNK_NAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'table_to_compress' and hypertable_schema = 'public'
ORDER BY chunk_name LIMIT 1
\gset

SELECT  compress_chunk( :'CHNAME');
SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME');

SELECT table_name, status 
FROM _timescaledb_catalog.chunk WHERE table_name = :'CHUNK_NAME';

--now chunk is frozen, cannot decompress
\set ON_ERROR_STOP 0
SELECT  decompress_chunk( :'CHNAME');
--insert into frozen chunk, should fail
INSERT INTO public.table_to_compress VALUES ('2020-01-01 10:00', 12, 77);
--touches all chunks
UPDATE public.table_to_compress SET value = 3; 
--touches only frozen chunk 
DELETE FROM public.table_to_compress WHERE time < '2020-01-02'; 
\set ON_ERROR_STOP 1
--try to refreeze
SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME');

--touches non-frozen chunk 
SELECT * from public.table_to_compress ORDER BY 1, 3;
DELETE FROM public.table_to_compress WHERE time > '2020-01-02'; 

SELECT * from public.table_to_compress ORDER BY 1, 3;

--TEST cannot drop frozen chunk, no error is reported.
-- simply skips
SELECT drop_chunks('table_to_compress', older_than=> '1 day'::interval);

--unfreeze and drop it
SELECT  _timescaledb_internal.unfreeze_chunk( :'CHNAME');
SELECT  _timescaledb_internal.drop_chunk( :'CHNAME');

--add a new chunk
INSERT INTO public.table_to_compress VALUES ('2019-01-01', 1234567, 777888);

--TEST  compress a frozen chunk fails
SELECT chunk_schema || '.' ||  chunk_name as "CHNAME", chunk_name as "CHUNK_NAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'table_to_compress' and hypertable_schema = 'public'
ORDER BY chunk_name DESC LIMIT 1
\gset

SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME');
\set ON_ERROR_STOP 0
SELECT  compress_chunk( :'CHNAME');
\set ON_ERROR_STOP 1

--TEST dropping a frozen chunk
--DO NOT CHANGE this behavior ---
-- frozen chunks cannot be dropped.

\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.drop_chunk(:'CHNAME');
\set ON_ERROR_STOP 1

--TEST drop_chunk in the presence of caggs. Does not affect cagg data
CREATE OR REPLACE FUNCTION hyper_dummy_now() RETURNS BIGINT
LANGUAGE SQL IMMUTABLE AS  'SELECT 100::BIGINT';
SELECT set_integer_now_func('test1.hyper1', 'hyper_dummy_now');

CREATE MATERIALIZED VIEW hyper1_cagg WITH (timescaledb.continuous)
AS SELECT time_bucket( 5, "time") as bucket, count(*)
FROM test1.hyper1 GROUP BY 1;
SELECT * FROM hyper1_cagg ORDER BY 1;

--now freeze chunk and try to  drop it
SELECT chunk_schema || '.' ||  chunk_name as "CHNAME1", chunk_name as "CHUNK_NAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'hyper1' and hypertable_schema = 'test1'
ORDER BY chunk_name LIMIT 1
\gset

SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME1');

--cannot drop frozen chunk
\set ON_ERROR_STOP 0
SELECT  _timescaledb_internal.drop_chunk( :'CHNAME1');
\set ON_ERROR_STOP 1

-- unfreeze the chunk, then drop the single chunk
SELECT  _timescaledb_internal.unfreeze_chunk( :'CHNAME1');

--drop the single chunk and verify that cagg is unaffected.
SELECT * FROM test1.hyper1 ORDER BY 1;

SELECT  _timescaledb_internal.drop_chunk( :'CHNAME1');

SELECT * from test1.hyper1 ORDER BY 1;
SELECT * FROM hyper1_cagg ORDER BY 1;

--TEST error case (un)freeze a non-chunk
CREATE TABLE nochunk_tab( a timestamp, b integer);
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.freeze_chunk('nochunk_tab');
SELECT _timescaledb_internal.unfreeze_chunk('nochunk_tab');
\set ON_ERROR_STOP 1

----- TESTS for attach_osm_table_chunk ------------
--TEST for attaching a foreign table as a chunk
--need superuser access to create foreign data server
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE DATABASE postgres_fdw_db;
GRANT ALL PRIVILEGES ON DATABASE postgres_fdw_db TO :ROLE_4;

\c postgres_fdw_db :ROLE_4
CREATE TABLE fdw_table( timec timestamptz NOT NULL , acq_id bigint, value bigint);
INSERT INTO fdw_table VALUES( '2020-01-01 01:00', 100, 1000);

--create foreign server and user mappings as superuser
\c :TEST_DBNAME :ROLE_SUPERUSER

SELECT current_setting('port') as "PORTNO" \gset

CREATE EXTENSION postgres_fdw;
CREATE SERVER s3_server FOREIGN DATA WRAPPER postgres_fdw 
OPTIONS ( host 'localhost', dbname 'postgres_fdw_db', port :'PORTNO');
GRANT USAGE ON FOREIGN SERVER s3_server TO :ROLE_4;

CREATE USER MAPPING FOR :ROLE_4 SERVER s3_server 
OPTIONS (  user :'ROLE_4' , password :'ROLE_4_PASS');

ALTER USER MAPPING FOR :ROLE_4 SERVER s3_server
OPTIONS (ADD password_required 'false');

\c :TEST_DBNAME :ROLE_4;
-- this is a stand-in for the OSM table
CREATE FOREIGN TABLE child_fdw_table
(timec timestamptz NOT NULL, acq_id bigint, value bigint)
 SERVER s3_server OPTIONS ( schema_name 'public', table_name 'fdw_table');

--now attach foreign table as a chunk of the hypertable.
CREATE TABLE ht_try(timec timestamptz NOT NULL, acq_id bigint, value bigint);
SELECT create_hypertable('ht_try', 'timec', chunk_time_interval => interval '1 day');
INSERT INTO ht_try VALUES ('2020-05-05 01:00', 222, 222);

SELECT * FROM child_fdw_table;

SELECT _timescaledb_internal.attach_osm_table_chunk('ht_try', 'child_fdw_table');

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'ht_try' ORDER BY 1;

SELECT * FROM ht_try ORDER BY 1;

SELECT relname, relowner FROM pg_class
WHERE relname in ( select chunk_name FROM timescaledb_information.chunks
                   WHERE hypertable_name = 'ht_try' ); 

SELECT inhrelid::regclass
FROM pg_inherits WHERE inhparent = 'ht_try'::regclass ORDER BY 1;

-- TEST error have to be hypertable owner to attach a chunk to it
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.attach_osm_table_chunk('ht_try', 'child_fdw_table');

-- TEST error try to attach to non hypertable
CREATE TABLE non_ht (time bigint, temp float);
SELECT _timescaledb_internal.attach_osm_table_chunk('non_ht', 'child_fdw_table');
 
\set ON_ERROR_STOP 1

-- TEST drop the hypertable and make sure foreign chunks are dropped as well --
\c :TEST_DBNAME :ROLE_4;
DROP TABLE ht_try;

SELECT relname FROM pg_class WHERE relname = 'child_fdw_table'; 

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'ht_try' ORDER BY 1;


-- TEST error try freeze/unfreeze on dist hypertable
-- Add distributed hypertables
\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2
\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => :'DN_DBNAME_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => :'DN_DBNAME_2');
CREATE TABLE disthyper (timec timestamp, device integer);
SELECT create_distributed_hypertable('disthyper', 'timec', 'device');
INSERT into disthyper VALUES ('2020-01-01', 10);

--freeze one of the chunks
SELECT chunk_schema || '.' ||  chunk_name as "CHNAME3"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'disthyper' 
ORDER BY chunk_name LIMIT 1
\gset

\set ON_ERROR_STOP 0
SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME3');
SELECT  _timescaledb_internal.unfreeze_chunk( :'CHNAME3');
\set ON_ERROR_STOP 1
SET ROLE :ROLE_DEFAULT_PERM_USER
