-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--  These tests work for PG14 or greater

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

--TEST error freeze a non-chunk
CREATE TABLE nochunk_tab( a timestamp, b integer);
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.freeze_chunk('nochunk_tab');
\set ON_ERROR_STOP 1

