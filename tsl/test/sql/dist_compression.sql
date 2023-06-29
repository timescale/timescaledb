-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

---------------------------------------------------
-- Test compression on a distributed hypertable
---------------------------------------------------
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3

\ir include/remote_exec.sql
\ir include/compression_utils.sql

SELECT node_name, database, node_created, database_created, extension_created
FROM (
  SELECT (add_data_node(name, host => 'localhost', DATABASE => name)).*
  FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v(name)
) a;

GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO :ROLE_1;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;
SET ROLE :ROLE_1;

CREATE TABLE compressed(time timestamptz, device int, temp float);
-- Replicate twice to see that compress_chunk compresses all replica chunks
SELECT create_distributed_hypertable('compressed', 'time', 'device', replication_factor => 2);
INSERT INTO compressed SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, random()*80
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-04 1:00', '1 hour') t;
ALTER TABLE compressed SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby = 'time DESC');

SELECT table_name, compression_state, compressed_hypertable_id
FROM _timescaledb_catalog.hypertable
ORDER BY 1;

SELECT * FROM timescaledb_information.compression_settings order by attname;
\x
SELECT * FROM _timescaledb_catalog.hypertable
WHERE table_name = 'compressed';
\x

SELECT test.remote_exec(NULL, $$
SELECT table_name, compressed_hypertable_id
FROM _timescaledb_catalog.hypertable
WHERE table_name = 'compressed';
$$);

-- There should be no compressed chunks
SELECT * from chunk_compression_stats( 'compressed')
ORDER BY chunk_name, node_name;

-- Test that compression is rolled back on aborted transaction
BEGIN;
SELECT compress_chunk(chunk)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk
LIMIT 1;

-- Data nodes should now report compressed chunks
SELECT * from chunk_compression_stats( 'compressed')
ORDER BY chunk_name, node_name;
-- Abort the transaction
ROLLBACK;

-- No compressed chunks since we rolled back
SELECT * from chunk_compression_stats( 'compressed')
ORDER BY chunk_name, node_name;

-- Compress for real this time
SELECT compress_chunk(chunk)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk
LIMIT 1;

-- Check that one chunk, and its replica, is compressed
SELECT * from chunk_compression_stats( 'compressed')
ORDER BY chunk_name, node_name;
select * from hypertable_compression_stats('compressed');

--- Decompress the chunk and replica
SELECT decompress_chunk(chunk)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk
LIMIT 1;

-- Compress odd numbered chunks
SELECT compress_chunk(chunk) FROM
(
  SELECT *, row_number() OVER () AS rownum
  FROM show_chunks('compressed') AS chunk
  ORDER BY chunk
) AS t
WHERE rownum % 2 = 1;
SELECT * from chunk_compression_stats('compressed')
ORDER BY chunk_name, node_name;

-- Compress twice to notice idempotent operation
SELECT compress_chunk(chunk, if_not_compressed => true)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk;

-- Compress again to verify errors are ignored
SELECT compress_chunk(chunk, if_not_compressed => true)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk;

-- There should be no uncompressed chunks
SELECT * from chunk_compression_stats('compressed')
ORDER BY chunk_name, node_name;
SELECT test.remote_exec(NULL, $$
SELECT * FROM _timescaledb_catalog.chunk ORDER BY id;
$$);

-- Decompress the chunks and replicas
SELECT decompress_chunk(chunk)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk;

-- Should now be decompressed
SELECT * from chunk_compression_stats('compressed')
ORDER BY chunk_name, node_name;

-- Decompress twice to generate NOTICE that the chunk is already decompressed
SELECT decompress_chunk(chunk, if_compressed => true)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk
LIMIT 1;

\x
SELECT * FROM timescaledb_information.hypertables
WHERE hypertable_name = 'compressed';
SELECT * from timescaledb_information.chunks
ORDER BY hypertable_name, chunk_name;
SELECT * from timescaledb_information.dimensions
ORDER BY hypertable_name, dimension_number;
\x

SELECT * FROM chunks_detailed_size('compressed'::regclass)
ORDER BY chunk_name, node_name;
SELECT * FROM hypertable_detailed_size('compressed'::regclass) ORDER BY node_name;

-- Disable compression on distributed table tests
ALTER TABLE compressed SET (timescaledb.compress = false);

SELECT table_name, compression_state, compressed_hypertable_id
FROM _timescaledb_catalog.hypertable
ORDER BY 1;

SELECT * FROM timescaledb_information.compression_settings order by attname;

--Now re-enable compression
ALTER TABLE compressed SET (timescaledb.compress, timescaledb.compress_segmentby='device');
SELECT table_name, compression_state, compressed_hypertable_id
FROM _timescaledb_catalog.hypertable
ORDER BY 1;
SELECT * FROM timescaledb_information.compression_settings order by attname;
SELECT compress_chunk(chunk, if_not_compressed => true)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk
LIMIT 1;

SELECT chunk_name, node_name, compression_status
FROM chunk_compression_stats('compressed')
ORDER BY 1, 2;

-- ALTER TABLE on distributed compressed hypertable
ALTER TABLE compressed ADD COLUMN new_coli integer;
ALTER TABLE compressed ADD COLUMN new_colv varchar(30);

SELECT * FROM _timescaledb_catalog.hypertable_compression
ORDER BY attname;

SELECT count(*) from compressed where new_coli is not null;

--insert data into new chunk
INSERT INTO compressed
SELECT '2019-08-01 00:00',  100, 100, 1, 'newcolv' ;

SELECT COUNT(*) AS count_compressed
FROM
(
SELECT compress_chunk(chunk.schema_name|| '.' || chunk.table_name, true)
FROM _timescaledb_catalog.chunk chunk
INNER JOIN _timescaledb_catalog.hypertable hypertable ON (chunk.hypertable_id = hypertable.id)
WHERE hypertable.table_name like 'compressed' and chunk.status & 1 = 0 ORDER BY chunk.id
)
AS sub;

SELECT * from compressed where new_coli is not null;

-- Test ALTER TABLE rename column on distributed hypertables
ALTER TABLE compressed RENAME new_coli TO new_intcol  ;
ALTER TABLE compressed RENAME device TO device_id  ;

SELECT * FROM test.remote_exec( NULL,
    $$ SELECT * FROM _timescaledb_catalog.hypertable_compression
       WHERE attname = 'device_id' OR attname = 'new_intcol'  and
       hypertable_id = (SELECT id from _timescaledb_catalog.hypertable
                       WHERE table_name = 'compressed' ) ORDER BY attname; $$ );

-- TEST insert data into compressed chunk
INSERT INTO compressed
SELECT '2019-08-01 01:00',  300, 300, 3, 'newcolv' ;
SELECT * from compressed where new_intcol = 3;

-- We're done with the table, so drop it.
DROP TABLE IF EXISTS compressed CASCADE;

------------------------------------------------------
-- Test compression policy on a distributed hypertable
------------------------------------------------------

CREATE TABLE conditions (
      time        TIMESTAMPTZ       NOT NULL,
      location    TEXT              NOT NULL,
      location2    char(10)              NOT NULL,
      temperature DOUBLE PRECISION  NULL,
      humidity    DOUBLE PRECISION  NULL
    );
SELECT create_distributed_hypertable('conditions', 'time', chunk_time_interval => '31days'::interval, replication_factor => 2);

--TEST 1--
--cannot set policy without enabling compression --
\set ON_ERROR_STOP 0
select add_compression_policy('conditions', '60d'::interval);
\set ON_ERROR_STOP 1

-- TEST2 --
--add a policy to compress chunks --
alter table conditions set (timescaledb.compress, timescaledb.compress_segmentby = 'location', timescaledb.compress_orderby = 'time');
insert into conditions
select generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day'), 'POR', 'klick', 55, 75;

select add_compression_policy('conditions', '60d'::interval) AS compressjob_id
\gset

select * from _timescaledb_config.bgw_job where id = :compressjob_id;
select * from alter_job(:compressjob_id, schedule_interval=>'1s');
select * from _timescaledb_config.bgw_job where id >= 1000 ORDER BY id;
-- we want only 1 chunk to be compressed --
SELECT alter_job(id,config:=jsonb_set(config,'{maxchunks_to_compress}', '1'))
FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

insert into conditions
select now()::timestamp, 'TOK', 'sony', 55, 75;

-- TEST3 --
--only the old chunks will get compressed when policy is executed--
CALL run_job(:compressjob_id);
select chunk_name, node_name, pg_size_pretty(before_compression_total_bytes) before_total,
pg_size_pretty( after_compression_total_bytes)  after_total
from chunk_compression_stats('conditions') where compression_status like 'Compressed' order by chunk_name;
SELECT * FROM _timescaledb_catalog.chunk ORDER BY id;

-- TEST 4 --
--cannot set another policy
\set ON_ERROR_STOP 0
select add_compression_policy('conditions', '60d'::interval, if_not_exists=>true);
select add_compression_policy('conditions', '60d'::interval);
select add_compression_policy('conditions', '30d'::interval, if_not_exists=>true);
\set ON_ERROR_STOP 1

--TEST 5 --
-- drop the policy --
select remove_compression_policy('conditions');
select count(*) from _timescaledb_config.bgw_job WHERE id>=1000;

--TEST 6 --
-- try to execute the policy after it has been dropped --
\set ON_ERROR_STOP 0
CALL run_job(:compressjob_id);
\set ON_ERROR_STOP 1

-- We're done with the table, so drop it.
DROP TABLE IF EXISTS conditions CASCADE;

--TEST 7
--compression policy for smallint, integer or bigint based partition hypertable
--smallint tests
CREATE TABLE test_table_smallint(time smallint, val int);
SELECT create_distributed_hypertable('test_table_smallint', 'time', chunk_time_interval => 1, replication_factor => 2);

CREATE OR REPLACE FUNCTION dummy_now_smallint() RETURNS SMALLINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::SMALLINT';
CALL distributed_exec($$
CREATE OR REPLACE FUNCTION dummy_now_smallint() RETURNS SMALLINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::SMALLINT'
$$);
SELECT set_integer_now_func('test_table_smallint', 'dummy_now_smallint');
INSERT INTO test_table_smallint SELECT generate_series(1,5), 10;
ALTER TABLE test_table_smallint SET (timescaledb.compress);
SELECT add_compression_policy('test_table_smallint', 2::int) AS compressjob_id \gset

SELECT * FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

CALL run_job(:compressjob_id);
CALL run_job(:compressjob_id);

SELECT chunk_name, node_name, before_compression_total_bytes, after_compression_total_bytes
FROM chunk_compression_stats('test_table_smallint')
WHERE compression_status LIKE 'Compressed'
ORDER BY chunk_name;

--integer tests
CREATE TABLE test_table_integer(time int, val int);
SELECT create_distributed_hypertable('test_table_integer', 'time', chunk_time_interval => 1, replication_factor => 2);

CREATE OR REPLACE FUNCTION dummy_now_integer() RETURNS INTEGER LANGUAGE SQL IMMUTABLE as  'SELECT 5::INTEGER';
CALL distributed_exec($$
CREATE OR REPLACE FUNCTION dummy_now_integer() RETURNS INTEGER LANGUAGE SQL IMMUTABLE as  'SELECT 5::INTEGER'
$$);
SELECT set_integer_now_func('test_table_integer', 'dummy_now_integer');
INSERT INTO test_table_integer SELECT generate_series(1,5), 10;
ALTER TABLE test_table_integer SET (timescaledb.compress);
SELECT add_compression_policy('test_table_integer', 2::int) AS compressjob_id \gset

SELECT * FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

CALL run_job(:compressjob_id);
CALL run_job(:compressjob_id);

SELECT chunk_name, node_name, before_compression_total_bytes, after_compression_total_bytes
FROM chunk_compression_stats('test_table_integer')
WHERE compression_status LIKE 'Compressed'
ORDER BY chunk_name;

--bigint tests
CREATE TABLE test_table_bigint(time bigint, val int);
SELECT create_distributed_hypertable('test_table_bigint', 'time', chunk_time_interval => 1, replication_factor => 2);

CREATE OR REPLACE FUNCTION dummy_now_bigint() RETURNS BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::BIGINT';
CALL distributed_exec($$
CREATE OR REPLACE FUNCTION dummy_now_bigint() RETURNS BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::BIGINT'
$$);
SELECT set_integer_now_func('test_table_bigint', 'dummy_now_bigint');
INSERT INTO test_table_bigint SELECT generate_series(1,5), 10;
ALTER TABLE test_table_bigint SET (timescaledb.compress);
SELECT add_compression_policy('test_table_bigint', 2::int) AS compressjob_id \gset

SELECT * FROM _timescaledb_config.bgw_job WHERE id = :compressjob_id;

CALL run_job(:compressjob_id);
CALL run_job(:compressjob_id);

SELECT chunk_name, node_name, before_compression_total_bytes, after_compression_total_bytes
FROM chunk_compression_stats('test_table_bigint')
WHERE compression_status LIKE 'Compressed'
ORDER BY chunk_name;

--TEST8 insert into compressed chunks on dist. hypertable
CREATE TABLE test_recomp_int(time bigint, val int);
SELECT create_distributed_hypertable('test_recomp_int', 'time', chunk_time_interval => 20);

CREATE OR REPLACE FUNCTION dummy_now() RETURNS BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 100::BIGINT';
CALL distributed_exec($$
CREATE OR REPLACE FUNCTION dummy_now() RETURNS BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 100::BIGINT'
$$);
select set_integer_now_func('test_recomp_int', 'dummy_now');
insert into test_recomp_int select generate_series(1,5), 10;
alter table test_recomp_int set (timescaledb.compress);

CREATE VIEW test_recomp_int_chunk_status as
SELECT
   c.table_name as chunk_name,
   c.status as chunk_status
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk c
WHERE h.id = c.hypertable_id and h.table_name = 'test_recomp_int';

--compress chunks
SELECT compress_chunk(chunk)
FROM show_chunks('test_recomp_int') AS chunk
ORDER BY chunk;

--check the status
SELECT * from test_recomp_int_chunk_status ORDER BY 1;

-- insert into compressed chunks of test_recomp_int (using same value for val)--
insert into test_recomp_int select 10, 10;
SELECT count(*) from test_recomp_int where val = 10;

--chunk status should change ---
SELECT * from test_recomp_int_chunk_status ORDER BY 1;

SELECT
c.schema_name || '.' || c.table_name as "CHUNK_NAME"
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk c
WHERE h.id = c.hypertable_id and h.table_name = 'test_recomp_int' \gset

--call recompress_chunk directly on distributed chunk
CALL recompress_chunk(:'CHUNK_NAME'::regclass);

--check chunk status now, should be compressed
SELECT * from test_recomp_int_chunk_status ORDER BY 1;
SELECT count(*) from test_recomp_int;

--add a policy--
select add_compression_policy('test_recomp_int', 1::int) AS compressjob_id
\gset
--once again add data to the compressed chunk
insert into test_recomp_int select generate_series(5,7), 10;
SELECT * from test_recomp_int_chunk_status ORDER BY 1;
--run the compression policy job, it will recompress chunks that are unordered
CALL run_job(:compressjob_id);
SELECT count(*) from test_recomp_int;

-- check with per datanode queries disabled
SET timescaledb.enable_per_data_node_queries TO false;
SELECT count(*) from test_recomp_int;
RESET timescaledb.enable_per_data_node_queries;

SELECT * from test_recomp_int_chunk_status ORDER BY 1;

---run copy tests
--copy data into existing chunk + for a new chunk
COPY test_recomp_int  FROM STDIN WITH DELIMITER ',';
11, 11
12, 12
13, 13
100, 100
101, 100
102, 100
\.

SELECT time_bucket(20, time ), count(*)
FROM test_recomp_int
GROUP BY time_bucket( 20, time) ORDER BY 1;

--another new chunk
INSERT INTO test_recomp_int VALUES( 65, 10);
SELECT * from test_recomp_int_chunk_status ORDER BY 1;

--compress all 3 chunks ---
--check status, unordered chunk status will not change
SELECT compress_chunk(chunk, true)
FROM show_chunks('test_recomp_int') AS chunk
ORDER BY chunk;
SELECT * from test_recomp_int_chunk_status ORDER BY 1;

--interleave copy into 3 different chunks and check status--
COPY test_recomp_int  FROM STDIN WITH DELIMITER ',';
14, 14
103, 100
66, 66
15, 15
104, 100
70, 70
\.

SELECT * from test_recomp_int_chunk_status ORDER BY 1;
SELECT time_bucket(20, time), count(*)
FROM test_recomp_int
GROUP BY time_bucket(20, time) ORDER BY 1;

-- check with per datanode queries disabled
SET timescaledb.enable_per_data_node_queries TO false;
SELECT time_bucket(20, time), count(*)
FROM test_recomp_int
GROUP BY time_bucket(20, time) ORDER BY 1;
RESET timescaledb.enable_per_data_node_queries;

--check compression_status afterwards--
CALL recompress_all_chunks('test_recomp_int', 2, true);
SELECT * from test_recomp_int_chunk_status ORDER BY 1;

CALL run_job(:compressjob_id);
SELECT * from test_recomp_int_chunk_status ORDER BY 1;

--verify that there are no errors if the policy/recompress_chunk is executed again
--on previously compressed chunks
CALL run_job(:compressjob_id);
CALL recompress_all_chunks('test_recomp_int', true);

--decompress and recompress chunk
\set ON_ERROR_STOP 0
SELECT decompress_chunk(chunk, true) FROM
( SELECT chunk FROM show_chunks('test_recomp_int') AS chunk ORDER BY chunk LIMIT 1 )q;
CALL recompress_all_chunks('test_recomp_int', 1, false);
\set ON_ERROR_STOP 1

-- test alter column type with distributed hypertable
\set ON_ERROR_STOP 0
ALTER TABLE test_table_smallint ALTER COLUMN val TYPE float;
ALTER TABLE test_table_integer ALTER COLUMN val TYPE float;
ALTER TABLE test_table_bigint ALTER COLUMN val TYPE float;
\set ON_ERROR_STOP 1

--create a cont agg view on the ht with compressed chunks as well
SELECT compress_chunk(chunk, true) FROM
( SELECT chunk FROM show_chunks('test_recomp_int') AS chunk ORDER BY chunk LIMIT 1 )q;

CREATE MATERIALIZED VIEW test_recomp_int_cont_view
WITH (timescaledb.continuous,
      timescaledb.materialized_only=true)
AS SELECT time_bucket(BIGINT '5', "time"), SUM(val)
   FROM test_recomp_int
   GROUP BY 1 WITH NO DATA;
SELECT add_continuous_aggregate_policy('test_recomp_int_cont_view', NULL, BIGINT '5', '1 day'::interval);
CALL refresh_continuous_aggregate('test_recomp_int_cont_view', NULL, NULL);
SELECT * FROM test_recomp_int ORDER BY 1;
SELECT * FROM test_recomp_int_cont_view ORDER BY 1;

--TEST cagg triggers work on distributed hypertables when we insert into
-- compressed chunks.
SELECT
CASE WHEN compress_chunk(chunk, true) IS NOT NULL THEN 'compress' END AS ch
 FROM
( SELECT chunk FROM show_chunks('test_recomp_int') AS chunk ORDER BY chunk )q;

SELECT * FROM test_recomp_int_cont_view
WHERE time_bucket = 0 or time_bucket = 50 ORDER BY 1;

--insert into an existing compressed chunk and a new chunk
SET timescaledb.enable_distributed_insert_with_copy=false;

INSERT INTO test_recomp_int VALUES (1, 10), (2,10), (3, 10);
INSERT INTO test_recomp_int VALUES(51, 10);

--refresh the cagg and verify the new results
CALL refresh_continuous_aggregate('test_recomp_int_cont_view', NULL, 100);
SELECT * FROM test_recomp_int_cont_view
WHERE time_bucket = 0 or time_bucket = 50 ORDER BY 1;

--repeat test with copy setting turned to true
SET timescaledb.enable_distributed_insert_with_copy=true;
INSERT INTO test_recomp_int VALUES (4, 10);

CALL refresh_continuous_aggregate('test_recomp_int_cont_view', NULL, 100);
SELECT * FROM test_recomp_int_cont_view
WHERE time_bucket = 0 or time_bucket = 50 ORDER BY 1;

--TEST drop one of the compressed chunks in test_recomp_int. The catalog
--tuple for the chunk will be preserved since we have a cagg.
-- Verify that status is accurate.
SELECT
   c.table_name as chunk_name,
   c.status as chunk_status, c.dropped, c.compressed_chunk_id as comp_id
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk c
WHERE h.id = c.hypertable_id and h.table_name = 'test_recomp_int' ORDER BY 1;
SELECT drop_chunks('test_recomp_int', older_than=> 20::bigint );
SELECT
   c.table_name as chunk_name,
   c.status as chunk_status, c.dropped, c.compressed_chunk_id as comp_id
FROM _timescaledb_catalog.hypertable h, _timescaledb_catalog.chunk c
WHERE h.id = c.hypertable_id and h.table_name = 'test_recomp_int' ORDER BY 1;

-- TEST drop should nuke everything
DROP TABLE test_recomp_int CASCADE;

-- test compression default handling
CREATE TABLE test_defaults(time timestamptz NOT NULL, device_id int);
SELECT create_distributed_hypertable('test_defaults','time');
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

-- test dropping columns from compressed
CREATE TABLE test_drop(f1 text, f2 text, f3 text, time timestamptz, device int, o1 text, o2 text);
SELECT create_distributed_hypertable('test_drop','time');
ALTER TABLE test_drop SET (timescaledb.compress,timescaledb.compress_segmentby='device',timescaledb.compress_orderby='o1,o2');

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

-- test ADD COLUMN IF NOT EXISTS on a distributed hypertable
CREATE TABLE metric (time TIMESTAMPTZ NOT NULL, val FLOAT8 NOT NULL, dev_id INT4 NOT NULL);

SELECT create_distributed_hypertable('metric', 'time');
ALTER TABLE metric SET (
timescaledb.compress,
timescaledb.compress_segmentby = 'dev_id',
timescaledb.compress_orderby = 'time DESC'
);

INSERT INTO metric(time, val, dev_id)
SELECT s.*, 3.14+1, 1
FROM generate_series('2021-07-01 00:00:00'::timestamp,
                    '2021-08-17 00:02:00'::timestamp, '30 s'::interval) s;

SELECT compress_chunk(chunk)
FROM show_chunks('metric') AS chunk
ORDER BY chunk;

-- make sure we have chunks on all data nodes
select * from timescaledb_information.chunks where hypertable_name like 'metric';
-- perform all combinations
-- [IF NOT EXISTS] - []
ALTER TABLE metric ADD COLUMN IF NOT EXISTS "medium" varchar;
-- [IF NOT EXISTS] - ["medium"]
ALTER TABLE metric ADD COLUMN IF NOT EXISTS "medium" varchar;
-- [] - []
ALTER TABLE metric ADD COLUMN "medium_1" varchar;
-- [] - ["medium_1"]
\set ON_ERROR_STOP 0
ALTER TABLE metric ADD COLUMN "medium_1" varchar;

SELECT * FROM metric limit 5;
SELECT * FROM metric where medium is not null;

-- INSERTs operate normally on the added column
INSERT INTO metric (time, val, dev_id, medium)
SELECT s.*, 3.14+1, 1, 'medium_value_text'
FROM generate_series('2021-08-18 00:00:00'::timestamp,
                    '2021-08-19 00:02:00'::timestamp, '30 s'::interval) s;

SELECT * FROM metric where medium is not null ORDER BY time LIMIT 1;

-- test alter_data_node(unvailable) with compressed chunks
--

-- create compressed distributed hypertable
CREATE TABLE compressed(time timestamptz NOT NULL, device int, temp float);
SELECT create_distributed_hypertable('compressed', 'time', 'device', replication_factor => 3);

-- insert data and get chunks distribution
INSERT INTO compressed SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, random()*80
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-04 1:00', '1 hour') t;
ALTER TABLE compressed SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby = 'time DESC');

SELECT compress_chunk(chunk)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk;
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('compressed'); $$);

SELECT chunk_schema || '.' ||  chunk_name, data_nodes
FROM timescaledb_information.chunks
WHERE hypertable_name = 'compressed';

SELECT count(*) FROM compressed;

-- make data node unavailable
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SELECT alter_data_node(:'DATA_NODE_1', port => 55433, available => false);
SET ROLE :ROLE_1;

-- update compressed chunks
INSERT INTO compressed SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, random()*80
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-04 1:00', '1 hour') t;

-- ensure that chunks associated with unavailable data node 1
-- are removed after being updated
SELECT chunk_schema || '.' ||  chunk_name, data_nodes
FROM timescaledb_information.chunks
WHERE hypertable_name = 'compressed';

SELECT * from show_chunks('compressed');
SELECT * FROM test.remote_exec(ARRAY[:'DATA_NODE_2', :'DATA_NODE_3'], $$ SELECT * from show_chunks('compressed'); $$);

SELECT count(*) FROM compressed;

-- make data node available again
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SELECT alter_data_node(:'DATA_NODE_1', port => 55432);
SELECT alter_data_node(:'DATA_NODE_1', available => true);
SET ROLE :ROLE_1;

-- ensure that stale chunks being dropped from data node 1 automatically
SELECT * from show_chunks('compressed');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('compressed'); $$);

SELECT count(*) FROM compressed;

-- recompress chunks
CALL recompress_all_chunks('compressed', 3, true);

SELECT count(*) FROM compressed;
SELECT * from show_chunks('compressed');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('compressed'); $$);

DROP TABLE compressed;

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;

