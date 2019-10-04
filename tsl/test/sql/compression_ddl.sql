-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/rand_generator.sql
\c :TEST_DBNAME :ROLE_SUPERUSER
\ir include/compression_utils.sql


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
WHERE hypertable.table_name like 'test1' and chunk.compressed_chunk_id IS NULL ORDER BY chunk.id
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

\set ON_ERROR_STOP 0
--set tablespace is blocked on chunks for now; if this ever changes we need
--to test that path to make sure it passes down the tablespace.
ALTER TABLE :UNCOMPRESSED_CHUNK_NAME SET TABLESPACE tablespace1;

SELECT move_chunk(chunk=>:'UNCOMPRESSED_CHUNK_NAME', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace1',  reorder_index=>'_timescaledb_internal."_hyper_1_1_chunk_test1_Time_idx"');
\set ON_ERROR_STOP 1

SELECT move_chunk(chunk=>:'COMPRESSED_CHUNK_NAME', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace1',  reorder_index=>'_timescaledb_internal."compress_hyper_2_28_chunk__compressed_hypertable_2_b__ts_meta_s"');

-- the compressed chunk is in here now
SELECT count(*)
FROM pg_tables WHERE tablespace = 'tablespace1';

SELECT decompress_chunk(:'UNCOMPRESSED_CHUNK_NAME');

--the compresse chunk was dropped by decompression
SELECT count(*)
FROM pg_tables WHERE tablespace = 'tablespace1';

SELECT move_chunk(chunk=>:'UNCOMPRESSED_CHUNK_NAME', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace1',  reorder_index=>'_timescaledb_internal."_hyper_1_1_chunk_test1_Time_idx"');

--the uncompressed chunks has now been moved
SELECT count(*)
FROM pg_tables WHERE tablespace = 'tablespace1';

SELECT compress_chunk(:'UNCOMPRESSED_CHUNK_NAME');

--the compressed chunk is now in the same tablespace as the uncompressed one
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

SELECT drop_chunks(table_name=>'test1', older_than => '2018-03-10'::TIMESTAMPTZ);

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
--errors due to dependent objects
SELECT drop_chunks(table_name=>'test1', older_than => '2018-03-28'::TIMESTAMPTZ);
\set ON_ERROR_STOP 1


SELECT drop_chunks(table_name=>'test1', older_than => '2018-03-28'::TIMESTAMPTZ, cascade=>true);

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

DROP TABLE :UNCOMPRESSED_HYPER_NAME;

--verify that there are no more hypertable remaining
SELECT count(*) FROM _timescaledb_catalog.hypertable hypertable;
SELECT count(*) FROM _timescaledb_catalog.hypertable_compression;
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
--TODO put back when cont aggs work
--CREATE VIEW test1_cont_view WITH ( timescaledb.continuous, timescaledb.refresh_interval='72 hours')
--AS SELECT time_bucket('1 hour', "Time"), SUM(i)
--    FROM test1
--    GROUP BY 1;

--REFRESH MATERIALIZED VIEW test1_cont_view;

--SELECT count(*) FROM test1_cont_view;
--DROP TABLE :UNCOMPRESSED_HYPER_NAME CASCADE;
--verify that there are no more hypertable remaining
--SELECT count(*) FROM _timescaledb_catalog.hypertable hypertable;

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

DROP TABLE test1;
DROP TABLESPACE tablespace1;
DROP TABLESPACE tablespace2;
