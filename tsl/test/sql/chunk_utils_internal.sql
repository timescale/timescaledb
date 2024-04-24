-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--  These tests work for PG14 or greater
-- Remember to corordinate any changes to functionality with the Cloud
-- Storage team. Tests for the following API:
-- * freeze_chunk
-- * drop_chunk
-- * attach_foreign_table_chunk
-- * hypertable_osm_range_update

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

CREATE MATERIALIZED VIEW hyper1_cagg WITH (timescaledb.continuous, timescaledb.materialized_only=false)
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
-- test size functions on foreign table
SELECT * FROM _timescaledb_functions.relation_approximate_size('child_fdw_table');
SELECT * FROM _timescaledb_functions.relation_size('child_fdw_table');

-- error should be thrown as the hypertable does not yet have an associated tiered chunk
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.hypertable_osm_range_update('ht_try','2020-01-01 01:00'::timestamptz, '2020-01-01 03:00');
\set ON_ERROR_STOP 1

SELECT _timescaledb_functions.attach_osm_table_chunk('ht_try', 'child_fdw_table');
-- check hypertable status
SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'ht_try';
-- must also update the range since the created chunk contains data
SELECT _timescaledb_functions.hypertable_osm_range_update('ht_try', '2020-01-01'::timestamptz, '2020-01-02');

-- OSM chunk is not visible in chunks view
SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'ht_try' ORDER BY 1;

SELECT chunk_name, range_start, range_end
FROM chunk_view
WHERE hypertable_name = 'ht_try'
ORDER BY chunk_name;

SELECT * FROM ht_try ORDER BY 1;

-- Check that the direct select from OSM chunk doesn't lead to bad effects.
SELECT * FROM child_fdw_table;

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

-- test ordered append
BEGIN;
-- before updating the ranges
EXPLAIN SELECT * FROM ht_try ORDER BY 1;
-- range before update
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.table_name = 'child_fdw_table' AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id;

SELECT _timescaledb_functions.hypertable_osm_range_update('ht_try', '2020-01-01 01:00'::timestamptz, '2020-01-02');
SELECT id, schema_name, table_name, status FROM _timescaledb_catalog.hypertable WHERE table_name = 'ht_try';
-- verify range was updated
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.table_name = 'child_fdw_table' AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id;
-- should be ordered append now
EXPLAIN SELECT * FROM ht_try ORDER BY 1;
SELECT * FROM ht_try ORDER BY 1;
-- test invalid range - should not be ordered append
SELECT _timescaledb_functions.hypertable_osm_range_update('ht_try');
EXPLAIN SELECT * from ht_try ORDER BY 1;
SELECT * from ht_try ORDER BY 1;
ROLLBACK;

\set ON_ERROR_STOP 0
-- test that error is produced when range_start < range_end
SELECT _timescaledb_functions.hypertable_osm_range_update('ht_try', '2020-01-02 01:00'::timestamptz, '2020-01-02 00:00');
-- error when range overlaps
SELECT _timescaledb_functions.hypertable_osm_range_update('ht_try', '2022-05-05 01:00'::timestamptz, '2022-05-06');
\set ON_ERROR_STOP 1

-- test that approximate size function works when a osm chunk is present
SELECT * FROM hypertable_approximate_size('ht_try');

--TEST GUC variable to enable/disable OSM chunk
SET timescaledb.enable_tiered_reads=false;
EXPLAIN (COSTS OFF) SELECT * from ht_try;
EXPLAIN (COSTS OFF) SELECT * from ht_try WHERE timec > '2022-01-01 01:00';
EXPLAIN (COSTS OFF) SELECT * from ht_try WHERE timec < '2023-01-01 01:00';
SET timescaledb.enable_tiered_reads=true;
EXPLAIN (COSTS OFF) SELECT * from ht_try;
-- foreign chunk contains data from Jan 2020, so it is skipped during planning
EXPLAIN (COSTS OFF) SELECT * from ht_try WHERE timec > '2022-01-01 01:00';
EXPLAIN (COSTS OFF) SELECT * from ht_try WHERE timec < '2023-01-01 01:00';

-- This test verifies that a bugfix regarding the way `ROWID_VAR`s are adjusted
-- in the chunks' targetlists on DELETE/UPDATE works (including partially
-- compressed chunks)
ALTER table ht_try SET (timescaledb.compress);
INSERT INTO ht_try VALUES ('2021-06-05 01:00', 10, 222);
SELECT compress_chunk(show_chunks('ht_try', newer_than => '2021-01-01'::timestamptz));
INSERT INTO ht_try VALUES ('2021-06-05 01:00', 10, 222);
DO $$
DECLARE
    r RECORD;
BEGIN
	EXPLAIN UPDATE ht_try SET value = 2
	WHERE acq_id = 10 AND timec > now() - '15 years'::interval INTO r;
END
$$ LANGUAGE plpgsql;

-- Check that the direct select from OSM chunk doesn't lead to bad effects in
-- presence of compression.
SELECT * FROM child_fdw_table;

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
-- was attached with data, so must update the range
SELECT _timescaledb_functions.hypertable_osm_range_update('hyper_constr', 100, 110);

SELECT table_name, status, osm_chunk
FROM _timescaledb_catalog.chunk
WHERE hypertable_id IN (SELECT id from _timescaledb_catalog.hypertable
                        WHERE table_name = 'hyper_constr')
ORDER BY table_name;

SELECT * FROM hyper_constr order by time;

--verify the check constraint exists on the OSM chunk
SELECT conname FROM pg_constraint
where conrelid = 'child_hyper_constr'::regclass ORDER BY 1;

--TEST retention policy is applied on OSM chunk by calling registered callback
CREATE OR REPLACE FUNCTION dummy_now_smallint() RETURNS BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 500::bigint' ;

SELECT set_integer_now_func('hyper_constr', 'dummy_now_smallint');
SELECT add_retention_policy('hyper_constr', 100::int) AS deljob_id \gset

--add hooks for osm callbacks that are triggered when drop_chunks is invoked---
SELECT ts_setup_osm_hook();
BEGIN;
SELECT drop_chunks('hyper_constr', 10::int);
SELECT id, table_name FROM _timescaledb_catalog.chunk
where hypertable_id = (Select id from _timescaledb_catalog.hypertable where table_name = 'hyper_constr')
ORDER BY id;
-- show_chunks will not show the OSM chunk which is visible via the above query
SELECT show_chunks('hyper_constr');
ROLLBACK;
CALL run_job(:deljob_id);
CALL run_job(:deljob_id);
SELECT chunk_name, range_start, range_end
FROM chunk_view
WHERE hypertable_name = 'hyper_constr'
ORDER BY chunk_name;
SELECT ts_undo_osm_hook();

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

-- test range of dimension slice for osm chunk for different datatypes
CREATE TABLE osm_int2(time int2 NOT NULL);
CREATE TABLE osm_int4(time int4 NOT NULL);
CREATE TABLE osm_int8(time int8 NOT NULL);
CREATE TABLE osm_date(time date NOT NULL);
CREATE TABLE osm_ts(time timestamp NOT NULL);
CREATE TABLE osm_tstz(time timestamptz NOT NULL);

SELECT table_name FROM create_hypertable('osm_int2','time',chunk_time_interval:=1000);
SELECT table_name FROM create_hypertable('osm_int4','time',chunk_time_interval:=1000);
SELECT table_name FROM create_hypertable('osm_int8','time',chunk_time_interval:=1000);
SELECT table_name FROM create_hypertable('osm_date','time');
SELECT table_name FROM create_hypertable('osm_ts','time');
SELECT table_name FROM create_hypertable('osm_tstz','time');

CREATE FOREIGN TABLE osm_int2_fdw_child(time int2 NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_int4_fdw_child(time int4 NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_int8_fdw_child(time int8 NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_date_fdw_child(time date NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_ts_fdw_child(time timestamp NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_tstz_fdw_child(time timestamptz NOT NULL) SERVER s3_server;

SELECT dt, _timescaledb_functions.attach_osm_table_chunk('osm_' || dt, 'osm_' || dt || '_fdw_child') FROM unnest('{int2,int4,int8,date,ts,tstz}'::text[]) u(dt);

SELECT ht.table_name, ds.*
FROM _timescaledb_catalog.dimension_slice ds
INNER JOIN _timescaledb_catalog.dimension d ON d.id=ds.dimension_id
INNER JOIN _timescaledb_catalog.hypertable ht on ht.id=d.hypertable_id
WHERE ht.table_name LIKE 'osm%'
ORDER BY 2,3;
-- test that correct slice is found and updated for table with multiple chunk constraints
CREATE TABLE test_multicon(time timestamptz not null unique, a int);
SELECT hypertable_id as htid FROM create_hypertable('test_multicon', 'time', chunk_time_interval => interval '1 day') \gset
insert into test_multicon values ('2020-01-02 01:00'::timestamptz, 1);
SELECT c.id, c.hypertable_id, c.schema_name, c.table_name, c.compressed_chunk_id, c.dropped, c.status, c.osm_chunk,
cc.chunk_id, cc.dimension_slice_id, cc.constraint_name, cc.hypertable_constraint_name FROM
_timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc WHERE c.hypertable_id = :htid
AND c.id = cc.chunk_id;
\c :TEST_DBNAME :ROLE_SUPERUSER ;
UPDATE _timescaledb_catalog.chunk SET osm_chunk = true WHERE hypertable_id = :htid;
\c :TEST_DBNAME :ROLE_4;
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon', '2020-01-02 01:00'::timestamptz, '2020-01-04 01:00');
-- view udpated range
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id;
-- check that range was reset to default - infinity
\set ON_ERROR_STOP 0
-- both range_start and range_end must be NULL, or non-NULL
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon', NULL, '2020-01-04 01:00'::timestamptz);
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon', NULL, NULL);
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon');
\set ON_ERROR_STOP 1
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon', NULL::timestamptz, NULL);
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;

-- test further with ordered append
\c postgres_fdw_db :ROLE_4;
CREATE TABLE test_chunkapp_fdw (time timestamptz NOT NULL, a int);
INSERT INTO test_chunkapp_fdw (time, a) VALUES ('2020-01-03 02:00'::timestamptz, 3);

\c :TEST_DBNAME :ROLE_4
CREATE TABLE test_chunkapp(time timestamptz NOT NULL, a int);
SELECT hypertable_id as htid FROM create_hypertable('test_chunkapp', 'time', chunk_time_interval => interval '1day') \gset
INSERT INTO test_chunkapp (time, a) VALUES ('2020-01-01 01:00'::timestamptz, 1), ('2020-01-02 01:00'::timestamptz, 2);

CREATE FOREIGN TABLE test_chunkapp_fdw_child(time timestamptz NOT NULL, a int) SERVER s3_server OPTIONS (schema_name 'public', table_name 'test_chunkapp_fdw');;
SELECT _timescaledb_functions.attach_osm_table_chunk('test_chunkapp','test_chunkapp_fdw_child');
-- view range before update
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;
-- attempt to update overlapping range, should fail
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.hypertable_osm_range_update('test_chunkapp', '2020-01-02 01:00'::timestamptz, '2020-01-04 01:00');
\set ON_ERROR_STOP 1
-- update actual range of OSM chunk, should work
SELECT _timescaledb_functions.hypertable_osm_range_update('test_chunkapp', '2020-01-03 00:00'::timestamptz, '2020-01-04 00:00');
-- view udpated range
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;
-- ordered append should be possible as ranges do not overlap
EXPLAIN SELECT * FROM test_chunkapp ORDER BY 1;
SELECT * FROM test_chunkapp ORDER BY 1;
-- but, insert should not be possible
SELECT ts_setup_osm_hook();
\set ON_ERROR_STOP 0
INSERT INTO test_chunkapp VALUES ('2020-01-03 02:00'::timestamptz, 3);
\set ON_ERROR_STOP 1
SELECT ts_undo_osm_hook();
-- reset range to infinity
SELECT _timescaledb_functions.hypertable_osm_range_update('test_chunkapp',empty:=false);
-- ordered append not possible because range is invalid and empty was not specified
EXPLAIN SELECT * FROM test_chunkapp ORDER BY 1;
SELECT * FROM test_chunkapp ORDER BY 1;
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;
-- but also, OSM chunk should be included in the scan, since range is invalid and chunk is not empty
EXPLAIN SELECT * FROM test_chunkapp WHERE time < '2023-01-01' ORDER BY 1;
SELECT * FROM test_chunkapp WHERE time < '2023-01-01' ORDER BY 1;
-- now set empty to true, should ordered append
\c postgres_fdw_db :ROLE_4;
DELETE FROM test_chunkapp_fdw;
\c :TEST_DBNAME :ROLE_4;
SELECT _timescaledb_functions.hypertable_osm_range_update('test_chunkapp', NULL::timestamptz, NULL, empty => true);
EXPLAIN SELECT * FROM test_chunkapp ORDER BY 1;
SELECT * FROM test_chunkapp ORDER BY 1;
-- should exclude the OSM chunk this time since it is empty
EXPLAIN SELECT * FROM test_chunkapp WHERE time < '2023-01-01' ORDER BY 1;
SELECT * FROM test_chunkapp WHERE time < '2023-01-01' ORDER BY 1;

\set ON_ERROR_STOP 0
-- test adding constraint directly on OSM chunk is blocked
ALTER TABLE test_chunkapp_fdw_child ADD CHECK (a > 0); -- non-dimensional
ALTER TABLE test_chunkapp_fdw_child ADD CHECK (time > '1600-01-01'::timestamptz); -- dimensional
-- but on hypertable, it is allowed
ALTER TABLE test_chunkapp ADD CHECK (a > 0);
\d+ test_chunkapp_fdw_child
\set ON_ERROR_STOP 1

-- test error is triggered when time dimension not found
CREATE TABLE test2(time timestamptz not null, a int);
SELECT create_hypertable('test2', 'time');
INSERT INTO test2 VALUES ('2020-01-01'::timestamptz, 1);
ALTER TABLE test2 SET (timescaledb.compress);
SELECT compress_chunk(show_chunks('test2'));
-- find internal compression table, call API function on it
SELECT format('%I.%I', cht.schema_name, cht.table_name) AS "COMPRESSION_TBLNM"
FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.hypertable cht
WHERE ht.table_name = 'test2' and cht.id = ht.compressed_hypertable_id \gset
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.hypertable_osm_range_update(:'COMPRESSION_TBLNM'::regclass, '2020-01-01'::timestamptz);
\set ON_ERROR_STOP 1

-- test wrong/incompatible data types with hypertable time dimension
-- update range of int2 with int4
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.hypertable_osm_range_update('osm_int2', range_start => 65540::int4, range_end => 100000::int4);
-- update range of int8 with int4
SELECT _timescaledb_functions.hypertable_osm_range_update('osm_int8', 120, 150);
-- update range of timestamptz with date
SELECT _timescaledb_functions.hypertable_osm_range_update('osm_tstz', '2020-01-01'::date, '2020-01-03'::date);
-- udpate range of timestamp with bigint
SELECT _timescaledb_functions.hypertable_osm_range_update('osm_tstz', 9223372036854771806, 9223372036854775406);
\set ON_ERROR_STOP 1

-- test dimension slice tuple visibility
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE TABLE osm_slice_update(time int not null);
SELECT hypertable_id AS ht_id FROM create_hypertable('osm_slice_update', 'time', chunk_time_interval => 10) \gset

INSERT INTO osm_slice_update VALUES (1);
UPDATE _timescaledb_catalog.hypertable SET status = 3 WHERE id = :ht_id;
UPDATE _timescaledb_catalog.chunk SET osm_chunk = true WHERE hypertable_id = :ht_id;

\c
BEGIN;
 	SELECT _timescaledb_functions.hypertable_osm_range_update('osm_slice_update',40,50);
ROLLBACK;

\c
-- new session should not be affected by previous rolled back transaction
-- should show 0 10 as range
\set ON_ERROR_STOP 0
INSERT INTO osm_slice_update VALUES (1);
INSERT INTO osm_slice_update VALUES (1);
\set ON_ERROR_STOP 1

-- test the drop_osm_table_chunk function
-- should not appear in timescale catalogs, or PG catalogs, or in the inheritance tree
\c :TEST_DBNAME :ROLE_4;
\d+ test_chunkapp
select id as htid from _timescaledb_catalog.hypertable where table_name = 'test_chunkapp' \gset
select status from _timescaledb_catalog.hypertable where table_name = 'test_chunkapp'; -- status should be 1
SELECT FROM _timescaledb_catalog.chunk ch, _timescaledb_catalog.hypertable ht
WHERE ht.table_name = 'test_chunkapp' and ht.id = ch.hypertable_id AND ch.osm_chunk IS TRUE; -- expect 1 entry
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;
-- it is empty so let's drop it
select * from test_chunkapp_fdw_child;
\set ON_ERROR_STOP 0
begin;
select _timescaledb_functions.drop_osm_table_chunk('osm_int2', 'test_chunkapp_fdw_child');
rollback;
\set ON_ERROR_STOP 1

select _timescaledb_functions.drop_osm_table_chunk('test_chunkapp', 'test_chunkapp_fdw_child');
-- foreign chunk no longer appears in the inheritance hierarchy
\d+ test_chunkapp
-- hypertable status is reset
select status from _timescaledb_catalog.hypertable where table_name = 'test_chunkapp'; -- status 0
SELECT FROM _timescaledb_catalog.chunk ch, _timescaledb_catalog.hypertable ht
WHERE ht.table_name = 'test_chunkapp' and ht.id = ch.hypertable_id AND ch.osm_chunk IS TRUE; -- expect 0 entries

-- chunk constraint and dimension slice should have been cleaned up as well
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;
-- test selects work as expected
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT * FROM test_chunkapp;

-- clean up databases created
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE postgres_fdw_db WITH (FORCE);
