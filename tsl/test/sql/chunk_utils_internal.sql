-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--  These tests work for PG14 or greater
-- Remember to corordinate any changes to functionality with the Cloud
-- Storage team. Tests for the following API:
-- * freeze_chunk
-- * drop_chunk
-- * attach_foreign_table_chunk

CREATE OR REPLACE VIEW chunk_view AS
  SELECT
    ht.table_name AS hypertable_name,
    srcch.schema_name AS schema_name,
    srcch.table_name AS chunk_name,
    _timescaledb_functions.to_timestamp(dimsl.range_start)
     AS range_start,
    _timescaledb_functions.to_timestamp(dimsl.range_end)
     AS range_end
  FROM _timescaledb_catalog.chunk srcch
    INNER JOIN _timescaledb_catalog.hypertable ht ON ht.id = srcch.hypertable_id
    INNER JOIN _timescaledb_catalog.chunk_constraint chcons ON srcch.id = chcons.chunk_id
    INNER JOIN _timescaledb_catalog.dimension dim ON srcch.hypertable_id = dim.hypertable_id
    INNER JOIN _timescaledb_catalog.dimension_slice dimsl ON dim.id = dimsl.dimension_id
      AND chcons.dimension_slice_id = dimsl.id;
GRANT SELECT on chunk_view TO PUBLIC;

\c :TEST_DBNAME :ROLE_SUPERUSER
-- fake presence of timescaledb_osm
INSERT INTO pg_extension(oid,extname,extowner,extnamespace,extrelocatable,extversion) SELECT 1,'timescaledb_osm',10,11,false,'1.0';

CREATE SCHEMA test1;
GRANT CREATE ON SCHEMA test1 TO :ROLE_DEFAULT_PERM_USER;
GRANT USAGE ON SCHEMA test1 TO :ROLE_DEFAULT_PERM_USER;

-- mock hooks for OSM interaction with timescaledb
CREATE OR REPLACE FUNCTION ts_setup_osm_hook( ) RETURNS VOID
AS :TSL_MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_undo_osm_hook( ) RETURNS VOID
AS :TSL_MODULE_PATHNAME LANGUAGE C VOLATILE;

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

-- Freeze
SELECT  _timescaledb_functions.freeze_chunk( :'CHNAME');

SELECT * from test1.hyper1 ORDER BY 1;

-- TEST updates and deletes on frozen chunk should fail
\set ON_ERROR_STOP 0

SELECT * from test1.hyper1 ORDER BY 1;

-- Value (time = 20) does not exist
UPDATE test1.hyper1 SET temp = 40 WHERE time = 20;
-- Frozen chunk is affected
UPDATE test1.hyper1 SET temp = 40 WHERE temp = 0.5;
-- Frozen chunk is affected
UPDATE test1.hyper1 SET temp = 40 WHERE time = 10;
-- Frozen chunk is affected
DELETE FROM test1.hyper1 WHERE time = 10;

SELECT * from test1.hyper1 ORDER BY 1;

BEGIN;
DELETE FROM test1.hyper1 WHERE time = 20;
DELETE FROM test1.hyper1 WHERE temp = 0.5;
ROLLBACK;

-- TEST update on unfrozen chunk should be possible
BEGIN;
SELECT * FROM test1.hyper1;
UPDATE test1.hyper1 SET temp = 40 WHERE time = 30;
SELECT * FROM test1.hyper1;
ROLLBACK;

-- Test with cast (chunk path pruning can not be done during query planning)
BEGIN;
SELECT * FROM test1.hyper1 WHERE time = 30;
UPDATE test1.hyper1 SET temp = 40 WHERE time = 30::text::float;
SELECT * FROM test1.hyper1 WHERE time = 30;
ROLLBACK;

-- TEST delete on unfrozen chunks should be possible
BEGIN;
SELECT * from test1.hyper1 ORDER BY 1;
DELETE FROM test1.hyper1 WHERE time = 30;
SELECT * from test1.hyper1 ORDER BY 1;
ROLLBACK;

-- Test with cast
BEGIN;
SELECT * FROM test1.hyper1 WHERE time = 30;
DELETE FROM test1.hyper1 WHERE time = 30::text::float;
SELECT * FROM test1.hyper1 WHERE time = 30;
ROLLBACK;

-- TEST inserts into a frozen chunk fails
INSERT INTO test1.hyper1 VALUES ( 11, 11);

-- Test truncating table should fail
TRUNCATE :CHNAME;

SELECT * from test1.hyper1 ORDER BY 1;

\set ON_ERROR_STOP 1

--insert into non-frozen chunk works
INSERT INTO test1.hyper1 VALUES ( 31, 31);
SELECT * from test1.hyper1 ORDER BY 1;

-- TEST unfreeze frozen chunk and then drop
SELECT table_name, status
FROM _timescaledb_catalog.chunk WHERE table_name = :'CHUNK_NAME';

SELECT  _timescaledb_functions.unfreeze_chunk( :'CHNAME');

SELECT tgname, tgtype FROM pg_trigger WHERE tgrelid = :'CHNAME'::regclass ORDER BY tgname, tgtype;

--verify status in catalog
SELECT table_name, status
FROM _timescaledb_catalog.chunk WHERE table_name = :'CHUNK_NAME';

-- Test update works after unfreeze
UPDATE test1.hyper1 SET temp = 40;

-- Test delete works after unfreeze
DELETE FROM test1.hyper1;

--unfreezing again works
SELECT  _timescaledb_functions.unfreeze_chunk( :'CHNAME');
SELECT  _timescaledb_functions.drop_chunk( :'CHNAME');

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
SELECT  _timescaledb_functions.freeze_chunk( :'CHNAME');

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
SELECT  _timescaledb_functions.freeze_chunk( :'CHNAME');

--touches non-frozen chunk
SELECT * from public.table_to_compress ORDER BY 1, 3;
DELETE FROM public.table_to_compress WHERE time > '2020-01-02';

SELECT * from public.table_to_compress ORDER BY 1, 3;

--TEST cannot drop frozen chunk, no error is reported.
-- simply skips
SELECT drop_chunks('table_to_compress', older_than=> '1 day'::interval);

--unfreeze and drop it
SELECT  _timescaledb_functions.unfreeze_chunk( :'CHNAME');
SELECT  _timescaledb_functions.drop_chunk( :'CHNAME');

--add a new chunk
INSERT INTO public.table_to_compress VALUES ('2019-01-01', 1234567, 777888);

--TEST  compress a frozen chunk fails
SELECT chunk_schema || '.' ||  chunk_name as "CHNAME", chunk_name as "CHUNK_NAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'table_to_compress' and hypertable_schema = 'public'
ORDER BY chunk_name DESC LIMIT 1
\gset

SELECT  _timescaledb_functions.freeze_chunk( :'CHNAME');
\set ON_ERROR_STOP 0
SELECT  compress_chunk( :'CHNAME');
\set ON_ERROR_STOP 1

--TEST dropping a frozen chunk
--DO NOT CHANGE this behavior ---
-- frozen chunks cannot be dropped.

\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.drop_chunk(:'CHNAME');
\set ON_ERROR_STOP 1

-- Prepare table for CAGG tests
TRUNCATE test1.hyper1;
INSERT INTO test1.hyper1(time, temp) values(30, 0.5), (31, 31);

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

SELECT  _timescaledb_functions.freeze_chunk( :'CHNAME1');

--cannot drop frozen chunk
\set ON_ERROR_STOP 0
SELECT  _timescaledb_functions.drop_chunk( :'CHNAME1');
\set ON_ERROR_STOP 1

-- unfreeze the chunk, then drop the single chunk
SELECT  _timescaledb_functions.unfreeze_chunk( :'CHNAME1');

--drop the single chunk and verify that cagg is unaffected.
SELECT * FROM test1.hyper1 ORDER BY 1;

SELECT  _timescaledb_functions.drop_chunk( :'CHNAME1');

SELECT * from test1.hyper1 ORDER BY 1;
SELECT * FROM hyper1_cagg ORDER BY 1;

-- check that dropping cagg triggers OSM callback
SELECT ts_setup_osm_hook();
BEGIN;
DROP MATERIALIZED VIEW hyper1_cagg CASCADE;
DROP TABLE test1.hyper1;
ROLLBACK;
BEGIN;
DROP TABLE test1.hyper1 CASCADE;
ROLLBACK;
SELECT ts_undo_osm_hook();

--TEST error case (un)freeze a non-chunk
CREATE TABLE nochunk_tab( a timestamp, b integer);
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.freeze_chunk('nochunk_tab');
SELECT _timescaledb_functions.unfreeze_chunk('nochunk_tab');
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
INSERT INTO ht_try VALUES ('2022-05-05 01:00', 222, 222);

SELECT * FROM child_fdw_table;

SELECT _timescaledb_functions.attach_osm_table_chunk('ht_try', 'child_fdw_table');

-- OSM chunk is not visible in chunks view
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'ht_try' ORDER BY 1;

SELECT chunk_name, range_start, range_end
FROM chunk_view
WHERE hypertable_name = 'ht_try'
ORDER BY chunk_name;

SELECT * FROM ht_try ORDER BY 1;

SELECT relname, relowner::regrole FROM pg_class
WHERE relname in ( select chunk_name FROM chunk_view
                   WHERE hypertable_name = 'ht_try' )
ORDER BY relname;

SELECT inhrelid::regclass
FROM pg_inherits WHERE inhparent = 'ht_try'::regclass ORDER BY 1;

--TEST chunk exclusion code does not filter out OSM chunk
SELECT * from ht_try ORDER BY 1;
SELECT * from ht_try WHERE timec < '2022-01-01 01:00' ORDER BY 1;
SELECT * from ht_try WHERE timec = '2020-01-01 01:00' ORDER BY 1;
SELECT * from ht_try WHERE  timec > '2000-01-01 01:00' and timec < '2022-01-01 01:00' ORDER BY 1;

SELECT * from ht_try WHERE timec > '2020-01-01 01:00' ORDER BY 1;

--TEST GUC variable to enable/disable OSM chunk
SET timescaledb.enable_tiered_reads=false;
EXPLAIN (COSTS OFF) SELECT * from ht_try;
EXPLAIN (COSTS OFF) SELECT * from ht_try WHERE timec > '2022-01-01 01:00';
EXPLAIN (COSTS OFF) SELECT * from ht_try WHERE timec < '2023-01-01 01:00';
SET timescaledb.enable_tiered_reads=true;
EXPLAIN (COSTS OFF) SELECT * from ht_try;
EXPLAIN (COSTS OFF) SELECT * from ht_try WHERE timec > '2022-01-01 01:00';
EXPLAIN (COSTS OFF) SELECT * from ht_try WHERE timec < '2023-01-01 01:00';

--TEST insert into a OSM chunk fails. actually any insert will fail. But we just need
-- to mock the hook and make sure the timescaledb code works correctly.

SELECT ts_setup_osm_hook();
\set ON_ERROR_STOP 0
--the mock hook returns true always. so cannot create a new chunk on the hypertable
INSERT INTO ht_try VALUES ('2022-06-05 01:00', 222, 222);
\set ON_ERROR_STOP 1
SELECT ts_undo_osm_hook();

-- TEST error have to be hypertable owner to attach a chunk to it
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.attach_osm_table_chunk('ht_try', 'child_fdw_table');

-- TEST error try to attach to non hypertable
CREATE TABLE non_ht (time bigint, temp float);
SELECT _timescaledb_functions.attach_osm_table_chunk('non_ht', 'child_fdw_table');

\set ON_ERROR_STOP 1

-- TEST drop the hypertable and make sure foreign chunks are dropped as well --
\c :TEST_DBNAME :ROLE_4;
DROP TABLE ht_try;

SELECT relname FROM pg_class WHERE relname = 'child_fdw_table';

SELECT table_name, status, osm_chunk
FROM _timescaledb_catalog.chunk
WHERE hypertable_id IN (SELECT id from _timescaledb_catalog.hypertable
                        WHERE table_name = 'ht_try')
ORDER BY table_name;

-- TEST error try freeze/unfreeze on dist hypertable
-- Add distributed hypertables
\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\c :TEST_DBNAME :ROLE_SUPERUSER

SELECT node_name, database, node_created, database_created, extension_created
FROM (
  SELECT (add_data_node(name, host => 'localhost', DATABASE => name)).*
  FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2')) v(name)
) a;

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
SELECT  _timescaledb_functions.freeze_chunk( :'CHNAME3');
SELECT  _timescaledb_functions.unfreeze_chunk( :'CHNAME3');
\set ON_ERROR_STOP 1

-- TEST can create OSM chunk if there are constraints on the hypertable
\c :TEST_DBNAME :ROLE_4
CREATE TABLE measure( id integer PRIMARY KEY, mname varchar(10));
INSERT INTO measure VALUES( 1, 'temp');

CREATE TABLE hyper_constr  ( id integer, time bigint, temp float, mid integer
                             ,PRIMARY KEY (id, time)
                             ,FOREIGN KEY ( mid) REFERENCES measure(id)
                             ,CHECK ( temp > 10)
                           );

SELECT create_hypertable('hyper_constr', 'time', chunk_time_interval => 10);
INSERT INTO hyper_constr VALUES( 10, 200, 22, 1);

\c postgres_fdw_db :ROLE_4
CREATE TABLE fdw_hyper_constr(id integer, time bigint, temp float, mid integer);
INSERT INTO fdw_hyper_constr VALUES( 10, 100, 33, 1);

\c :TEST_DBNAME :ROLE_4
-- this is a stand-in for the OSM table
CREATE FOREIGN TABLE child_hyper_constr
( id integer NOT NULL, time bigint NOT NULL, temp float, mid integer)
 SERVER s3_server OPTIONS ( schema_name 'public', table_name 'fdw_hyper_constr');

--check constraints are automatically added for the foreign table
SELECT _timescaledb_functions.attach_osm_table_chunk('hyper_constr', 'child_hyper_constr');

SELECT table_name, status, osm_chunk
FROM _timescaledb_catalog.chunk
WHERE hypertable_id IN (SELECT id from _timescaledb_catalog.hypertable
                        WHERE table_name = 'hyper_constr')
ORDER BY table_name;

SELECT * FROM hyper_constr order by time;

--verify the check constraint exists on the OSM chunk
SELECT conname FROM pg_constraint
where conrelid = 'child_hyper_constr'::regclass ORDER BY 1;

--TEST policy is not applied on OSM chunk
CREATE OR REPLACE FUNCTION dummy_now_smallint() RETURNS BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 500::bigint' ;

SELECT set_integer_now_func('hyper_constr', 'dummy_now_smallint');
SELECT add_retention_policy('hyper_constr', 100::int) AS deljob_id \gset

CALL run_job(:deljob_id);
CALL run_job(:deljob_id);
SELECT chunk_name, range_start, range_end
FROM chunk_view
WHERE hypertable_name = 'hyper_constr'
ORDER BY chunk_name;

----- TESTS for copy into frozen chunk ------------
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
CREATE TABLE test1.copy_test (
    "time" timestamptz NOT NULL,
    "value" double precision NOT NULL
);

SELECT create_hypertable('test1.copy_test', 'time', chunk_time_interval => interval '1 day');

COPY test1.copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- Freeze one of the chunks
SELECT chunk_schema || '.' ||  chunk_name as "COPY_CHNAME", chunk_name as "COPY_CHUNK_NAME"
FROM timescaledb_information.chunks
WHERE hypertable_name = 'copy_test' and hypertable_schema = 'test1'
ORDER BY chunk_name LIMIT 1
\gset

SELECT _timescaledb_functions.freeze_chunk( :'COPY_CHNAME');

-- Check state
SELECT table_name, status
FROM _timescaledb_catalog.chunk WHERE table_name = :'COPY_CHUNK_NAME';

\set ON_ERROR_STOP 0
-- Copy should fail because one of che chunks is frozen
COPY test1.copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.
\set ON_ERROR_STOP 1

-- Count existing rows
SELECT COUNT(*) FROM test1.copy_test;

-- Check state
SELECT table_name, status
FROM _timescaledb_catalog.chunk WHERE table_name = :'COPY_CHUNK_NAME';

\set ON_ERROR_STOP 0
-- Copy should fail because one of che chunks is frozen
COPY test1.copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.
\set ON_ERROR_STOP 1

-- Count existing rows
SELECT COUNT(*) FROM test1.copy_test;

-- Check unfreeze restored chunk
SELECT _timescaledb_functions.unfreeze_chunk( :'COPY_CHNAME');

-- Check state
SELECT table_name, status
FROM _timescaledb_catalog.chunk WHERE table_name = :'COPY_CHUNK_NAME';

-- Copy should work now
COPY test1.copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

--Utility functions -check index creation on hypertable with OSM chunk
-- Indexes are not created on OSM chunks, they are skipped as these are foreign tables
\c :TEST_DBNAME :ROLE_4
CREATE INDEX hyper_constr_mid_idx ON hyper_constr( mid, time);
SELECT indexname, tablename FROM pg_indexes WHERE indexname = 'hyper_constr_mid_idx';
DROP INDEX hyper_constr_mid_idx;

CREATE INDEX hyper_constr_mid_idx ON hyper_constr(mid, time) WITH (timescaledb.transaction_per_chunk);
SELECT indexname, tablename FROM pg_indexes WHERE indexname = 'hyper_constr_mid_idx';
DROP INDEX hyper_constr_mid_idx;

-- clean up databases created
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE postgres_fdw_db;
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
