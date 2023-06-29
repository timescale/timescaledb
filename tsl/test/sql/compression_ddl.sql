-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/rand_generator.sql
\c :TEST_DBNAME :ROLE_SUPERUSER

SET client_min_messages = ERROR;
DROP TABLESPACE IF EXISTS tablespace1;
DROP TABLESPACE IF EXISTS tablespace2;
SET client_min_messages = NOTICE;
--test hypertable with tables space
CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;
CREATE TABLESPACE tablespace2 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE2_PATH;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE TABLE test1 ("Time" timestamptz, i integer, b bigint, t text);
SELECT table_name from create_hypertable('test1', 'Time', chunk_time_interval=> INTERVAL '1 day');

INSERT INTO test1 SELECT t,  gen_rand_minstd(), gen_rand_minstd(), gen_rand_minstd()::text FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') t;

ALTER TABLE test1 set (timescaledb.compress, timescaledb.compress_segmentby = 'b', timescaledb.compress_orderby = '"Time" DESC');

SELECT COUNT(*) AS count_compressed
FROM
(
SELECT compress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.status & 1 = 0 ORDER BY chunk.id
)
AS sub;


--make sure allowed ddl still work
ALTER TABLE test1 CLUSTER ON "test1_Time_idx";
ALTER TABLE test1 SET WITHOUT CLUSTER;
CREATE INDEX new_index ON test1(b);
DROP INDEX new_index;
ALTER TABLE test1 SET (fillfactor=100);
ALTER TABLE test1 RESET (fillfactor);
ALTER TABLE test1 ALTER COLUMN b SET STATISTICS 10;

-- make sure we cannot create constraints or unique indexes on compressed hypertables
\set ON_ERROR_STOP 0
ALTER TABLE test1 ADD CONSTRAINT c1 UNIQUE(time,i);
CREATE UNIQUE INDEX unique_index ON test1(time,i);
\set ON_ERROR_STOP 1

--test adding boolean columns with default and not null
CREATE TABLE records (time timestamp NOT NULL);
SELECT create_hypertable('records', 'time');
ALTER TABLE records SET (timescaledb.compress = true);
ALTER TABLE records ADD COLUMN col1 boolean DEFAULT false NOT NULL;
-- NULL constraints are useless and it is safe allow adding this
-- column with NULL constraint to a compressed hypertable (Issue #5151)
ALTER TABLE records ADD COLUMN col2 BOOLEAN NULL;
DROP table records CASCADE;

-- TABLESPACES
-- For tablepaces with compressed chunks the semantics are the following:
--  - compressed chunks get put into the same tablespace as the
--    uncompressed chunk on compression.
-- - set tablespace on uncompressed hypertable cascades to compressed hypertable+chunks
-- - set tablespace on all chunks is blocked (same as w/o compression)
-- - move chunks on a uncompressed chunk errors
-- - move chunks on compressed chunk works

--In the future we will:
-- - add tablespace option to compress_chunk function and policy (this will override the setting
--   of the uncompressed chunk). This will allow changing tablespaces upon compression
-- - Note: The current plan is to never listen to the setting on compressed hypertable. In fact,
--   we will block setting tablespace on  compressed hypertables


SELECT count(*) as "COUNT_CHUNKS_UNCOMPRESSED"
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' \gset

SELECT count(*) as "COUNT_CHUNKS_COMPRESSED"
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test1' \gset

ALTER TABLE test1 SET TABLESPACE tablespace1;

--all chunks + both the compressed and uncompressed hypertable moved to new tablespace
SELECT count(*) = (:COUNT_CHUNKS_UNCOMPRESSED +:COUNT_CHUNKS_COMPRESSED + 2)
FROM pg_tables WHERE tablespace = 'tablespace1';

ALTER TABLE test1 SET TABLESPACE tablespace2;
SELECT count(*) = (:COUNT_CHUNKS_UNCOMPRESSED +:COUNT_CHUNKS_COMPRESSED + 2)
FROM pg_tables WHERE tablespace = 'tablespace2';

SELECT
    comp_chunk.schema_name|| '.' || comp_chunk.table_name as "COMPRESSED_CHUNK_NAME",
    uncomp_chunk.schema_name|| '.' || uncomp_chunk.table_name as "UNCOMPRESSED_CHUNK_NAME"
FROM _timescaledb_catalog.chunk comp_chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (comp_chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
INNER JOIN _timescaledb_catalog.chunk uncomp_chunk ON (uncomp_chunk.compressed_chunk_id = comp_chunk.id)
WHERE uncomp_hyper.table_name like 'test1' ORDER BY comp_chunk.id LIMIT 1\gset

-- ensure compression chunk cannot be moved directly
SELECT tablename
FROM pg_tables WHERE tablespace = 'tablespace1';

\set ON_ERROR_STOP 0
ALTER TABLE :COMPRESSED_CHUNK_NAME SET TABLESPACE tablespace1;
\set ON_ERROR_STOP 1
SELECT tablename
FROM pg_tables WHERE tablespace = 'tablespace1';

-- ensure that both compressed and uncompressed chunks moved
ALTER TABLE :UNCOMPRESSED_CHUNK_NAME SET TABLESPACE tablespace1;
SELECT tablename
FROM pg_tables WHERE tablespace = 'tablespace1';

ALTER TABLE test1 SET TABLESPACE tablespace2;
SELECT tablename
FROM pg_tables WHERE tablespace = 'tablespace1';

\set ON_ERROR_STOP 0
SELECT move_chunk(chunk=>:'COMPRESSED_CHUNK_NAME', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace1',  reorder_index=>'_timescaledb_internal."compress_hyper_2_28_chunk__compressed_hypertable_2_b__ts_meta_s"');
\set ON_ERROR_STOP 1

-- ensure that both compressed and uncompressed chunks moved
SELECT move_chunk(chunk=>:'UNCOMPRESSED_CHUNK_NAME', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace1',  reorder_index=>'_timescaledb_internal."_hyper_1_1_chunk_test1_Time_idx"');
SELECT tablename
FROM pg_tables WHERE tablespace = 'tablespace1';

-- the compressed chunk is in here now
SELECT count(*)
FROM pg_tables WHERE tablespace = 'tablespace1';

SELECT decompress_chunk(:'UNCOMPRESSED_CHUNK_NAME');

--the compressed chunk remains (empty) after decompression
SELECT count(*)
FROM pg_tables WHERE tablespace = 'tablespace1';

SELECT move_chunk(chunk=>:'UNCOMPRESSED_CHUNK_NAME', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace1',  reorder_index=>'_timescaledb_internal."_hyper_1_1_chunk_test1_Time_idx"');

--the chunks has now been moved
SELECT count(*)
FROM pg_tables WHERE tablespace = 'tablespace1';

SELECT compress_chunk(:'UNCOMPRESSED_CHUNK_NAME');

--the compressed chunk is in the same tablespace as the uncompressed one, was moved previously
SELECT count(*)
FROM pg_tables WHERE tablespace = 'tablespace1';

--
-- DROP CHUNKS
--
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1';

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test1';


SELECT chunk.schema_name|| '.' || chunk.table_name as "UNCOMPRESSED_CHUNK_NAME"
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' ORDER BY chunk.id LIMIT 1 \gset

DROP TABLE :UNCOMPRESSED_CHUNK_NAME;

--should decrease #chunks both compressed and decompressed
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1';

--make sure there are no orphaned  _timescaledb_catalog.compression_chunk_size entries (should be 0)
SELECT count(*) as orphaned_compression_chunk_size
FROM _timescaledb_catalog.compression_chunk_size size
LEFT JOIN _timescaledb_catalog.chunk chunk ON (chunk.id = size.chunk_id)
WHERE chunk.id IS NULL;

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test1';

SELECT drop_chunks('test1', older_than => '2018-03-10'::TIMESTAMPTZ);

--should decrease #chunks both compressed and decompressed
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1';

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test1';

SELECT chunk.schema_name|| '.' || chunk.table_name as "UNCOMPRESSED_CHUNK_NAME"
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' ORDER BY chunk.id LIMIT 1 \gset

SELECT chunk.schema_name|| '.' || chunk.table_name as "COMPRESSED_CHUNK_NAME"
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test1' ORDER BY chunk.id LIMIT 1
\gset

\set ON_ERROR_STOP 0
DROP TABLE :COMPRESSED_CHUNK_NAME;
\set ON_ERROR_STOP 1

SELECT
    chunk.schema_name|| '.' || chunk.table_name as "UNCOMPRESSED_CHUNK_NAME",
    comp_chunk.schema_name|| '.' || comp_chunk.table_name as "COMPRESSED_CHUNK_NAME"
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.chunk comp_chunk ON (chunk.compressed_chunk_id = comp_chunk.id)
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' ORDER BY chunk.id LIMIT 1 \gset

--create a dependent object on the compressed chunk to test cascade behaviour
CREATE VIEW dependent_1 AS SELECT * FROM :COMPRESSED_CHUNK_NAME;

\set ON_ERROR_STOP 0
--errors due to dependent objects
DROP TABLE :UNCOMPRESSED_CHUNK_NAME;
\set ON_ERROR_STOP 1

DROP TABLE :UNCOMPRESSED_CHUNK_NAME CASCADE;

--should decrease #chunks both compressed and decompressed
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1';

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test1';

SELECT
    chunk.schema_name|| '.' || chunk.table_name as "UNCOMPRESSED_CHUNK_NAME",
    comp_chunk.schema_name|| '.' || comp_chunk.table_name as "COMPRESSED_CHUNK_NAME"
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.chunk comp_chunk ON (chunk.compressed_chunk_id = comp_chunk.id)
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' ORDER BY chunk.id LIMIT 1 \gset

CREATE VIEW dependent_1 AS SELECT * FROM :COMPRESSED_CHUNK_NAME;

\set ON_ERROR_STOP 0
\set VERBOSITY default
--errors due to dependent objects
SELECT drop_chunks('test1', older_than => '2018-03-28'::TIMESTAMPTZ);
\set VERBOSITY terse
\set ON_ERROR_STOP 1

DROP VIEW dependent_1;
SELECT drop_chunks('test1', older_than => '2018-03-28'::TIMESTAMPTZ);

--should decrease #chunks both compressed and decompressed
SELECT count(*) as count_chunks_uncompressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1';

SELECT count(*) as count_chunks_compressed
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test1';

--make sure there are no orphaned  _timescaledb_catalog.compression_chunk_size entries (should be 0)
SELECT count(*) as orphaned_compression_chunk_size
FROM _timescaledb_catalog.compression_chunk_size size
LEFT JOIN _timescaledb_catalog.chunk chunk ON (chunk.id = size.chunk_id)
WHERE chunk.id IS NULL;

--
-- DROP HYPERTABLE
--

SELECT comp_hyper.schema_name|| '.' || comp_hyper.table_name as "COMPRESSED_HYPER_NAME"
FROM _timescaledb_catalog.hypertable comp_hyper
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test1' ORDER BY comp_hyper.id LIMIT 1 \gset

\set ON_ERROR_STOP 0
DROP TABLE :COMPRESSED_HYPER_NAME;
\set ON_ERROR_STOP 1

BEGIN;
SELECT hypertable.schema_name|| '.' || hypertable.table_name as "UNCOMPRESSED_HYPER_NAME"
FROM _timescaledb_catalog.hypertable hypertable
WHERE hypertable.table_name like 'test1' ORDER BY hypertable.id LIMIT 1 \gset

--before the drop there are 2 hypertables: the compressed and uncompressed ones
SELECT count(*) FROM _timescaledb_catalog.hypertable hypertable;
--add policy to make sure it's dropped later
select add_compression_policy(:'UNCOMPRESSED_HYPER_NAME', interval '1 day');
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE id >= 1000;

DROP TABLE :UNCOMPRESSED_HYPER_NAME;

--verify that there are no more hypertable remaining
SELECT count(*) FROM _timescaledb_catalog.hypertable hypertable;
SELECT count(*) FROM _timescaledb_catalog.hypertable_compression;

--verify that the policy is gone
SELECT count(*) FROM _timescaledb_config.bgw_job WHERE id >= 1000;

ROLLBACK;

--create a dependent object on the compressed hypertable to test cascade behaviour

CREATE VIEW dependent_1 AS SELECT * FROM :COMPRESSED_HYPER_NAME;
\set ON_ERROR_STOP 0
DROP TABLE :UNCOMPRESSED_HYPER_NAME;
\set ON_ERROR_STOP 1

BEGIN;
DROP TABLE :UNCOMPRESSED_HYPER_NAME CASCADE;
SELECT count(*) FROM _timescaledb_catalog.hypertable hypertable;
ROLLBACK;
DROP VIEW dependent_1;


--create a cont agg view on the ht as well then the drop should nuke everything
CREATE MATERIALIZED VIEW test1_cont_view
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS SELECT time_bucket('1 hour', "Time"), SUM(i)
   FROM test1
   GROUP BY 1 WITH NO DATA;
SELECT add_continuous_aggregate_policy('test1_cont_view', NULL, '1 hour'::interval, '1 day'::interval);
CALL refresh_continuous_aggregate('test1_cont_view', NULL, NULL);

SELECT count(*) FROM test1_cont_view;

\c :TEST_DBNAME :ROLE_SUPERUSER

SELECT chunk.schema_name|| '.' || chunk.table_name as "COMPRESSED_CHUNK_NAME"
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable comp_hyper ON (chunk.hypertable_id = comp_hyper.id)
INNER JOIN _timescaledb_catalog.hypertable uncomp_hyper ON (comp_hyper.id = uncomp_hyper.compressed_hypertable_id)
WHERE uncomp_hyper.table_name like 'test1' ORDER BY chunk.id LIMIT 1
\gset

ALTER TABLE test1 OWNER TO :ROLE_DEFAULT_PERM_USER_2;

--make sure new owner is propagated down
SELECT a.rolname from pg_class c INNER JOIN pg_authid a ON(c.relowner = a.oid) WHERE c.oid = 'test1'::regclass;
SELECT a.rolname from pg_class c INNER JOIN pg_authid a ON(c.relowner = a.oid) WHERE c.oid = :'COMPRESSED_HYPER_NAME'::regclass;
SELECT a.rolname from pg_class c INNER JOIN pg_authid a ON(c.relowner = a.oid) WHERE c.oid = :'COMPRESSED_CHUNK_NAME'::regclass;

--
-- turn off compression
--

SELECT COUNT(*) AS count_compressed
FROM
(
SELECT decompress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.status & 1 > 0 ORDER BY chunk.id
)
AS sub;

select add_compression_policy('test1', interval '1 day');
\set ON_ERROR_STOP 0
ALTER table test1 set (timescaledb.compress='f');
\set ON_ERROR_STOP 1

select remove_compression_policy('test1');
ALTER table test1 set (timescaledb.compress='f');

--only one hypertable left
SELECT count(*) = 1 FROM _timescaledb_catalog.hypertable hypertable;
SELECT compressed_hypertable_id IS NULL FROM _timescaledb_catalog.hypertable hypertable WHERE hypertable.table_name like 'test1' ;
--no hypertable compression entries left
SELECT count(*) = 0 FROM _timescaledb_catalog.hypertable_compression;
--make sure there are no orphaned  _timescaledb_catalog.compression_chunk_size entries (should be 0)
SELECT count(*) as orphaned_compression_chunk_size
FROM _timescaledb_catalog.compression_chunk_size size
LEFT JOIN _timescaledb_catalog.chunk chunk ON (chunk.id = size.chunk_id)
WHERE chunk.id IS NULL;


--can turn compression back on
-- this will fail because current table owner does not have permission to create compressed hypertable
-- in the current tablespace, as they do not own it
\set ON_ERROR_STOP 0
ALTER TABLE test1 set (timescaledb.compress, timescaledb.compress_segmentby = 'b', timescaledb.compress_orderby = '"Time" DESC');
-- now change the owner and repeat, should work
ALTER TABLESPACE tablespace2 OWNER TO :ROLE_DEFAULT_PERM_USER_2;
\set ON_ERROR_STOP 1

ALTER TABLE test1 set (timescaledb.compress, timescaledb.compress_segmentby = 'b', timescaledb.compress_orderby = '"Time" DESC');

SELECT COUNT(*) AS count_compressed
FROM
(
SELECT compress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.status & 1 = 0 ORDER BY chunk.id
)
AS sub;

DROP TABLE test1 CASCADE;
DROP TABLESPACE tablespace1;

-- Triggers are NOT fired for compress/decompress
CREATE TABLE test1 ("Time" timestamptz, i integer);
SELECT table_name from create_hypertable('test1', 'Time', chunk_time_interval=> INTERVAL '1 day');
CREATE OR REPLACE FUNCTION test1_print_func()
RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
   RAISE NOTICE ' raise notice test1_print_trigger called ';
   RETURN OLD;
END;
$BODY$;
CREATE TRIGGER test1_trigger
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON test1
FOR EACH STATEMENT EXECUTE FUNCTION test1_print_func();

INSERT INTO test1 SELECT generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-03 1:00', '1 hour') , 1 ;
-- add a row trigger too --
CREATE TRIGGER test1_trigger2
BEFORE INSERT OR UPDATE OR DELETE ON test1
FOR EACH ROW EXECUTE FUNCTION test1_print_func();
INSERT INTO test1 SELECT '2018-03-02 1:05'::TIMESTAMPTZ, 2;

ALTER TABLE test1 set (timescaledb.compress, timescaledb.compress_orderby = '"Time" DESC');

SELECT COUNT(*) AS count_compressed FROM
(
SELECT compress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1' and chunk.status & 1 = 0 ORDER BY chunk.id) AS subq;

SELECT COUNT(*) AS count_compressed FROM
(
SELECT decompress_chunk(chunk.schema_name|| '.' || chunk.table_name)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'test1'  ORDER BY chunk.id ) as subq;

DROP TABLE test1;

-- test disabling compression on hypertables with caggs and dropped chunks
-- github issue 2844
CREATE TABLE i2844 (created_at timestamptz NOT NULL,c1 float);
SELECT create_hypertable('i2844', 'created_at', chunk_time_interval => '6 hour'::interval);
INSERT INTO i2844 SELECT generate_series('2000-01-01'::timestamptz, '2000-01-02'::timestamptz,'1h'::interval);

CREATE MATERIALIZED VIEW test_agg WITH (timescaledb.continuous) AS SELECT time_bucket('1 hour', created_at) AS bucket, AVG(c1) AS avg_c1 FROM i2844 GROUP BY bucket;

ALTER TABLE i2844 SET (timescaledb.compress);

SELECT compress_chunk(show_chunks) AS compressed_chunk FROM show_chunks('i2844');
SELECT drop_chunks('i2844', older_than => '2000-01-01 18:00'::timestamptz);
SELECT decompress_chunk(show_chunks, if_compressed => TRUE) AS decompressed_chunks FROM show_chunks('i2844');

ALTER TABLE i2844 SET (timescaledb.compress = FALSE);

-- TEST compression alter schema tests
\ir include/compression_alter.sql

--TEST tablespaces for compressed chunks with attach_tablespace interface --
CREATE TABLE test2 (timec timestamptz, i integer, t integer);
SELECT table_name from create_hypertable('test2', 'timec', chunk_time_interval=> INTERVAL '1 day');

SELECT attach_tablespace('tablespace2', 'test2');

INSERT INTO test2 SELECT t,  gen_rand_minstd(), 22
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-02 13:00', '1 hour') t;

ALTER TABLE test2 set (timescaledb.compress, timescaledb.compress_segmentby = 'i', timescaledb.compress_orderby = 'timec');

-- the toast table and its index will have a different name depending on
-- the order the tests run, so anonymize it
SELECT regexp_replace(relname, 'pg_toast_[0-9]+', 'pg_toast_XXX') FROM pg_class
WHERE reltablespace in
  ( SELECT oid from pg_tablespace WHERE spcname = 'tablespace2') ORDER BY 1;

-- test compress_chunk() with utility statement (SELECT ... INTO)
SELECT compress_chunk(ch) INTO compressed_chunks FROM show_chunks('test2') ch;
SELECT decompress_chunk(ch) INTO decompressed_chunks FROM show_chunks('test2') ch;

-- compress again
SELECT compress_chunk(ch) FROM show_chunks('test2') ch;

-- the chunk, compressed chunk + index + toast tables are in tablespace2 now .
-- toast table names differ across runs. So we use count to verify the results
-- instead of printing the table/index names
SELECT count(*) FROM (
SELECT relname FROM pg_class
WHERE reltablespace in
  ( SELECT oid from pg_tablespace WHERE spcname = 'tablespace2'))q;

DROP TABLE test2 CASCADE;
DROP TABLESPACE tablespace2;

-- Create a table with a compressed table and then delete the
-- compressed table and see that the drop of the hypertable does not
-- generate an error. This scenario can be triggered if an extension
-- is created with compressed hypertables since the tables are dropped
-- as part of the drop of the extension.
CREATE TABLE issue4140("time" timestamptz NOT NULL, device_id int);
SELECT create_hypertable('issue4140', 'time');
ALTER TABLE issue4140 SET(timescaledb.compress);
SELECT format('%I.%I', schema_name, table_name)::regclass AS ctable
FROM _timescaledb_catalog.hypertable
WHERE id = (SELECT compressed_hypertable_id FROM _timescaledb_catalog.hypertable WHERE table_name = 'issue4140') \gset
SELECT timescaledb_pre_restore();
DROP TABLE :ctable;
SELECT timescaledb_post_restore();
DROP TABLE issue4140;

-- github issue 5104
CREATE TABLE metric(
	time TIMESTAMPTZ NOT NULL,
	value DOUBLE PRECISION NOT NULL,
	series_id BIGINT NOT NULL);

SELECT create_hypertable('metric', 'time',
	chunk_time_interval => interval '1 h',
	create_default_indexes => false);

-- enable compression
ALTER TABLE metric set(timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id, value',
    timescaledb.compress_orderby = 'time'
);

SELECT
      comp_hypertable.schema_name AS "COMP_SCHEMA_NAME",
      comp_hypertable.table_name AS "COMP_TABLE_NAME"
FROM _timescaledb_catalog.hypertable uc_hypertable
INNER JOIN _timescaledb_catalog.hypertable comp_hypertable ON (comp_hypertable.id = uc_hypertable.compressed_hypertable_id)
WHERE uc_hypertable.table_name like 'metric' \gset

-- get definition of compressed hypertable and notice the index
\d :COMP_SCHEMA_NAME.:COMP_TABLE_NAME

-- #5290 Compression can't be enabled on caggs
CREATE TABLE "tEst2" (
    "Id" uuid NOT NULL,
    "Time" timestamp with time zone NOT NULL,
    CONSTRAINT "test2_pkey" PRIMARY KEY ("Id", "Time")
);

SELECT create_hypertable(
  '"tEst2"',
  'Time',
  chunk_time_interval => INTERVAL '1 day'
);

alter table "tEst2" set (timescaledb.compress=true, timescaledb.compress_segmentby='"Id"');

CREATE MATERIALIZED VIEW "tEst2_mv"
WITH (timescaledb.continuous) AS
SELECT "Id" as "Idd",
   time_bucket(INTERVAL '1 day', "Time") AS "bUcket"
FROM public."tEst2"
GROUP BY "Idd", "bUcket";

ALTER MATERIALIZED VIEW "tEst2_mv" SET (timescaledb.compress = true);


-- #5161 segmentby param
CREATE MATERIALIZED VIEW test1_cont_view2
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true
      )
AS SELECT time_bucket('1 hour', "Time") as t, SUM(intcol) as sum,txtcol as "iDeA"
   FROM test1
   GROUP BY 1,txtcol WITH NO DATA;


\set ON_ERROR_STOP 0
ALTER MATERIALIZED VIEW test1_cont_view2 SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = 'invalid_column'
);
\set ON_ERROR_STOP 1

ALTER MATERIALIZED VIEW test1_cont_view2 SET (
  timescaledb.compress = true
);

ALTER MATERIALIZED VIEW test1_cont_view2 SET (
  timescaledb.compress = true,
  timescaledb.compress_segmentby = '"iDeA"'
);

\set ON_ERROR_STOP 0
ALTER MATERIALIZED VIEW test1_cont_view2 SET (
  timescaledb.compress = true,
  timescaledb.compress_orderby = '"iDeA"'
);
\set ON_ERROR_STOP 1

ALTER MATERIALIZED VIEW test1_cont_view2 SET (
  timescaledb.compress = false
);

DROP TABLE metric CASCADE;

-- inserting into compressed chunks with different physical layouts
CREATE TABLE compression_insert(filler_1 int, filler_2 int, filler_3 int, time timestamptz NOT NULL, device_id int, v0 int, v1 int, v2 float, v3 float);
CREATE INDEX ON compression_insert(time);
CREATE INDEX ON compression_insert(device_id,time);
SELECT create_hypertable('compression_insert','time',create_default_indexes:=false);
ALTER TABLE compression_insert SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='device_id');

-- test without altering physical layout
-- this is a baseline test to compare results with
-- next series of tests which should yield identical results
-- while changing the physical layouts of chunks
INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-01 0:00:00+0'::timestamptz,'2000-01-03 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);

SELECT compress_chunk(c.schema_name|| '.' || c.table_name) as "CHUNK_NAME"
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht
WHERE c.hypertable_id = ht.id and ht.table_name = 'compression_insert'
AND c.status & 1 = 0
ORDER BY c.table_name DESC \gset

INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-04 0:00:00+0'::timestamptz,'2000-01-05 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-01 0:00:00+0'
AND time <= '2000-01-05 23:55:00+0';

-- force index scans to check index mapping
-- this verifies that we are actually using compressed chunk index scans
-- previously we could not use indexes on uncompressed chunks due to a bug:
-- https://github.com/timescale/timescaledb/issues/5432
--
-- this check basically makes sure that the indexes are built properly
-- and there are no issues in attribute mappings while building them
SET enable_seqscan = off;
EXPLAIN (costs off) SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
CALL recompress_chunk(:'CHUNK_NAME'::regclass);
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-01 0:00:00+0'
AND time <= '2000-01-05 23:55:00+0';
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SET enable_seqscan = default;

-- 1. drop column after first insert into chunk, before compressing
INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-07 0:00:00+0'::timestamptz,'2000-01-09 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);

ALTER TABLE compression_insert DROP COLUMN filler_1;
SELECT compress_chunk(c.schema_name|| '.' || c.table_name) as "CHUNK_NAME"
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht
WHERE c.hypertable_id = ht.id
AND ht.table_name = 'compression_insert'
AND c.status & 1 = 0
ORDER BY c.table_name DESC \gset

INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1,  device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-10 0:00:00+0'::timestamptz,'2000-01-11 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);

SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-07 0:00:00+0'
AND time <= '2000-01-11 23:55:00+0';

-- force index scans to check index mapping
SET enable_seqscan = off;
EXPLAIN (costs off) SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
CALL recompress_chunk(:'CHUNK_NAME'::regclass);
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-07 0:00:00+0'
AND time <= '2000-01-11 23:55:00+0';
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SET enable_seqscan = default;

-- 2. drop column after compressing chunk
INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-15 0:00:00+0'::timestamptz,'2000-01-17 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);

SELECT compress_chunk(c.schema_name|| '.' || c.table_name) as "CHUNK_NAME"
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht
WHERE c.hypertable_id = ht.id
AND ht.table_name = 'compression_insert'
AND c.status & 1 = 0
ORDER BY c.table_name DESC \gset

ALTER TABLE compression_insert DROP COLUMN filler_2;
INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-18 0:00:00+0'::timestamptz,'2000-01-19 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-15 0:00:00+0'
AND time <= '2000-01-19 23:55:00+0';

-- force index scans to check index mapping
SET enable_seqscan = off;
EXPLAIN (costs off) SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
CALL recompress_chunk(:'CHUNK_NAME'::regclass);
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-15 0:00:00+0'
AND time <= '2000-01-19 23:55:00+0';
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SET enable_seqscan = default;

-- 3. add new column after first insert into chunk, before compressing
INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-22 0:00:00+0'::timestamptz,'2000-01-24 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);

ALTER TABLE compression_insert ADD COLUMN filler_4 int;
SELECT compress_chunk(c.schema_name|| '.' || c.table_name) as "CHUNK_NAME"
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht
WHERE c.hypertable_id = ht.id
AND ht.table_name = 'compression_insert'
AND c.status & 1 = 0
ORDER BY c.table_name DESC \gset

INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-25 0:00:00+0'::timestamptz,'2000-01-26 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-22 0:00:00+0'
AND time <= '2000-01-26 23:55:00+0';

-- force index scans to check index mapping
SET enable_seqscan = off;
EXPLAIN (costs off) SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
CALL recompress_chunk(:'CHUNK_NAME'::regclass);
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-22 0:00:00+0'
AND time <= '2000-01-26 23:55:00+0';
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SET enable_seqscan = default;

-- 4. add new column after compressing chunk
INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-28 0:00:00+0'::timestamptz,'2000-01-30 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
SELECT compress_chunk(c.schema_name|| '.' || c.table_name) as "CHUNK_NAME"
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.hypertable ht
WHERE c.hypertable_id = ht.id
AND ht.table_name = 'compression_insert'
AND c.status & 1 = 0
ORDER BY c.table_name DESC \gset

ALTER TABLE compression_insert ADD COLUMN filler_5 int;
INSERT INTO compression_insert(time,device_id,v0,v1,v2,v3)
SELECT time, device_id, device_id+1, device_id + 2, device_id + 0.5, NULL
FROM generate_series('2000-01-31 0:00:00+0'::timestamptz,'2000-02-01 23:55:00+0','2m') gtime(time), generate_series(1,5,1) gdevice(device_id);
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-28 0:00:00+0'
AND time <= '2000-02-01 23:55:00+0';

-- force index scans to check index mapping
SET enable_seqscan = off;
EXPLAIN (costs off) SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
CALL recompress_chunk(:'CHUNK_NAME'::regclass);
SELECT count(*), sum(v0), sum(v1), sum(v2), sum(v3)
FROM compression_insert
WHERE time >= '2000-01-28 0:00:00+0'
AND time <= '2000-02-01 23:55:00+0';
SELECT device_id, count(*)
FROM compression_insert
GROUP BY device_id
ORDER BY device_id;
SET enable_seqscan = default;

DROP TABLE compression_insert;

-- check Chunk Append plans for partially compressed chunks
-- F: fully compressed, P : partially compressed, U: uncompressed
CREATE TABLE test_partials (time timestamptz NOT NULL, a int, b int);
SELECT create_hypertable('test_partials', 'time');
INSERT INTO test_partials
VALUES -- chunk1
  ('2020-01-01 00:00'::timestamptz, 1, 2),
  ('2020-01-01 00:01'::timestamptz, 2, 2),
  ('2020-01-01 00:04'::timestamptz, 1, 2),
  -- chunk2
  ('2021-01-01 00:00'::timestamptz, 1, 2),
  ('2021-01-01 00:04'::timestamptz, 1, 2),
  -- chunk3
  ('2022-01-01 00:00'::timestamptz, 1, 2),
  ('2022-01-01 00:04'::timestamptz, 1, 2);
-- enable compression, compress all chunks
ALTER TABLE test_partials SET (timescaledb.compress);
SELECT compress_chunk(show_chunks('test_partials'));
-- fully compressed
EXPLAIN (costs off) SELECT * FROM test_partials ORDER BY time;
-- test P, F, F
INSERT INTO test_partials VALUES ('2020-01-01 00:03', 1, 2);
EXPLAIN (costs off) SELECT * FROM test_partials ORDER BY time;
-- verify correct results
SELECT * FROM test_partials ORDER BY time;
-- make second chunk partially compressed
-- P, P, F
INSERT INTO test_partials VALUES ('2021-01-01 00:03', 1, 2);
EXPLAIN (costs off) SELECT * FROM test_partials ORDER BY time;
-- verify correct results
SELECT * FROM test_partials ORDER BY time;
-- third chunk partially compressed and add new chunk
-- P, P, P, U
INSERT INTO test_partials VALUES ('2022-01-01 00:03', 1, 2);
INSERT INTO test_partials VALUES ('2023-01-01 00:03', 1, 2);
EXPLAIN (costs off) SELECT * FROM test_partials ORDER BY time;
-- F, F, P, U
-- recompress all chunks
DO $$
DECLARE
  chunk regclass;
BEGIN
  FOR chunk IN
  SELECT format('%I.%I', schema_name, table_name)::regclass
    FROM _timescaledb_catalog.chunk WHERE status = 9 AND NOT dropped
  LOOP
    EXECUTE format('select decompress_chunk(''%s'');', chunk::text);
    EXECUTE format('select compress_chunk(''%s'');', chunk::text);
  END LOOP;
END
$$;
INSERT INTO test_partials VALUES ('2022-01-01 00:02', 1, 2);
EXPLAIN (COSTS OFF) SELECT * FROM test_partials ORDER BY time;
-- F, F, P, F, F
INSERT INTO test_partials VALUES ('2024-01-01 00:02', 1, 2);
SELECT compress_chunk(c) FROM show_chunks('test_partials', newer_than => '2022-01-01') c;
EXPLAIN (costs off) SELECT * FROM test_partials ORDER BY time;
-- verify result correctness
SELECT * FROM test_partials ORDER BY time;

-- add test for space partioning with partial chunks
CREATE TABLE space_part (time timestamptz, a int, b int, c int);
SELECT create_hypertable('space_part', 'time', chunk_time_interval => INTERVAL '1 day');

INSERT INTO space_part VALUES
-- chunk1
('2020-01-01 00:00', 1, 1, 1),
('2020-01-01 00:00', 2, 1, 1),
('2020-01-01 00:03', 1, 1, 1),
('2020-01-01 00:03', 2, 1, 1);
INSERT INTO space_part values
-- chunk2
('2021-01-01 00:00', 1, 1, 1),
('2021-01-01 00:00', 2, 1, 1),
('2021-01-01 00:03', 1, 1, 1),
('2021-01-01 00:03', 2, 1, 1);
-- compress them
ALTER TABLE space_part SET (timescaledb.compress);
SELECT compress_chunk(show_chunks('space_part'));
-- make first chunk partial
INSERT INTO space_part VALUES
-- chunk1
('2020-01-01 00:01', 1, 1, 1),
('2020-01-01 00:01', 2, 1, 1);

-------- now enable the space partitioning, this will take effect for chunks created subsequently
SELECT add_dimension('space_part', 'a', number_partitions => 5);
-- plan is still the same
EXPLAIN (COSTS OFF) SELECT * FROM space_part ORDER BY time;

-- now add more chunks that do adhere to the new space partitioning
-- chunks 3,4
INSERT INTO space_part VALUES
('2022-01-01 00:00', 1, 1, 1),
('2022-01-01 00:00', 2, 1, 1),
('2022-01-01 00:03', 1, 1, 1),
('2022-01-01 00:03', 2, 1, 1);
-- plan still ok
EXPLAIN (COSTS OFF) SELECT * FROM space_part ORDER BY time;
-- compress them
SELECT compress_chunk(c, if_not_compressed=>true) FROM show_chunks('space_part') c;
-- plan still ok
EXPLAIN (COSTS OFF) SELECT * FROM space_part ORDER BY time;
-- make second one of them partial
insert into space_part values
('2022-01-01 00:02', 2, 1, 1),
('2022-01-01 00:02', 2, 1, 1);
EXPLAIN (COSTS OFF) SELECT * FROM space_part ORDER BY time;
-- make other one partial too
INSERT INTO space_part VALUES
('2022-01-01 00:02', 1, 1, 1);
EXPLAIN (COSTS OFF) SELECT * FROM space_part ORDER BY time;
