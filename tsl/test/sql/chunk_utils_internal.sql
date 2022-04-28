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

--TEST internal api that freezes a chunk
--freeze one of the chunks
SELECT chunk_schema || '.' ||  chunk_name as "CHNAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'hyper1' and hypertable_schema = 'test1'
ORDER BY chunk_name LIMIT 1
\gset

SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME');

SELECT * from test1.hyper1 ORDER BY 1;

--updates and deletes on frozen chunk should fail
\set ON_ERROR_STOP 0
UPDATE test1.hyper1 SET temp = 40 WHERE time = 20;
UPDATE test1.hyper1 SET temp = 40 WHERE temp = 0.5;
SELECT * from test1.hyper1 ORDER BY 1;

DELETE FROM test1.hyper1 WHERE time = 20;
DELETE FROM test1.hyper1 WHERE temp = 0.5;
SELECT * from test1.hyper1 ORDER BY 1;

--inserts into a frozen chunk fails
INSERT INTO test1.hyper1 VALUES ( 11, 11);
--insert into non-frozen chunk works
INSERT INTO test1.hyper1 VALUES ( 31, 31);
SELECT * from test1.hyper1 ORDER BY 1;
\set ON_ERROR_STOP 1
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
--try to refreeze
SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME');
\set ON_ERROR_STOP 1

--touches non-frozen chunk 
SELECT * from public.table_to_compress ORDER BY 1, 3;
DELETE FROM public.table_to_compress WHERE time > '2020-01-02'; 

SELECT * from public.table_to_compress ORDER BY 1, 3;

--TEST can drop frozen chunk
SELECT drop_chunks('table_to_compress', older_than=> '1 day'::interval);
--add a new chunk
INSERT INTO public.table_to_compress VALUES ('2020-01-01', 1234567, 777888);
--TEST now feeeze and try to compress the chunk
SELECT chunk_schema || '.' ||  chunk_name as "CHNAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'table_to_compress' and hypertable_schema = 'public'
ORDER BY chunk_name LIMIT 1
\gset

SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME');
\set ON_ERROR_STOP 0
SELECT  compress_chunk( :'CHNAME');
\set ON_ERROR_STOP 1

--TEST dropping a frozen chunk
--DO NOT CHANGE this behavior ---
-- frozen chunks can be dropped.
SELECT _timescaledb_internal.drop_chunk(:'CHNAME');
SELECT count(*) 
FROM timescaledb_information.chunks
WHERE hypertable_name = 'table_to_compress' and hypertable_schema = 'public';

--TEST error freeze a non-chunk
CREATE TABLE nochunk_tab( a timestamp, b integer);
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.freeze_chunk('nochunk_tab');
\set ON_ERROR_STOP 1

--TEST dropping frozen chunk in the presence of caggs
SELECT * FROM test1.hyper1 ORDER BY 1;

CREATE OR REPLACE FUNCTION hyper_dummy_now() RETURNS BIGINT
LANGUAGE SQL IMMUTABLE AS  'SELECT 100::BIGINT';
SELECT set_integer_now_func('test1.hyper1', 'hyper_dummy_now');

CREATE MATERIALIZED VIEW hyper1_cagg WITH (timescaledb.continuous)
AS SELECT time_bucket( 5, "time") as bucket, count(*)
FROM test1.hyper1 GROUP BY 1;
SELECT * FROM hyper1_cagg ORDER BY 1;

--now freeze chunk and drop it
--cagg data is unaffected  
SELECT chunk_schema || '.' ||  chunk_name as "CHNAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'hyper1' and hypertable_schema = 'test1'
ORDER BY chunk_name LIMIT 1
\gset

SELECT  _timescaledb_internal.freeze_chunk( :'CHNAME');
SELECT  _timescaledb_internal.drop_chunk( :'CHNAME');
SELECT * from test1.hyper1 ORDER BY 1;
SELECT * FROM hyper1_cagg ORDER BY 1;

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

-- this is a stand-in for the OSM table
CREATE FOREIGN TABLE child_fdw_table
(timec timestamptz NOT NULL, acq_id bigint, value bigint)
 SERVER s3_server OPTIONS ( schema_name 'public', table_name 'fdw_table');
GRANT SELECT ON  child_fdw_table TO :ROLE_4;

--now attach foreign table as a chunk of the hypertable.
\c :TEST_DBNAME :ROLE_4;
CREATE TABLE ht_try(timec timestamptz NOT NULL, acq_id bigint, value bigint);
SELECT create_hypertable('ht_try', 'timec', chunk_time_interval => interval '1 day');
INSERT INTO ht_try VALUES ('2020-05-05 01:00', 222, 222);

SELECT * FROM child_fdw_table;

SELECT _timescaledb_internal.attach_osm_table_chunk('ht_try', 'child_fdw_table');

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'ht_try' ORDER BY 1;

SELECT * FROM ht_try ORDER BY 1;

-- TEST error have to be hypertable owner to attach a chunk to it
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.attach_osm_table_chunk('ht_try', 'child_fdw_table');

-- TEST error try to attach to non hypertable
CREATE TABLE non_ht (time bigint, temp float);
SELECT _timescaledb_internal.attach_osm_table_chunk('non_ht', 'child_fdw_table');
 
\set ON_ERROR_STOP 1
