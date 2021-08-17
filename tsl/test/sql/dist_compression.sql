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

SELECT (add_data_node (name, host => 'localhost', DATABASE => name)).*
FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v (name);

GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO :ROLE_1;
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
WHERE hypertable.table_name like 'compressed' and chunk.compressed_chunk_id IS NULL ORDER BY chunk.id
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
--compression policy for integer based partition hypertable
CREATE TABLE test_table_int(time bigint, val int);
SELECT create_distributed_hypertable('test_table_int', 'time', chunk_time_interval => 1, replication_factor => 2);

CREATE OR REPLACE FUNCTION dummy_now() RETURNS BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::BIGINT';
CALL distributed_exec($$
CREATE OR REPLACE FUNCTION dummy_now() RETURNS BIGINT LANGUAGE SQL IMMUTABLE as  'SELECT 5::BIGINT'
$$);
select set_integer_now_func('test_table_int', 'dummy_now');
insert into test_table_int select generate_series(1,5), 10;
alter table test_table_int set (timescaledb.compress);
select add_compression_policy('test_table_int', 2::int) AS compressjob_id
\gset

select * from _timescaledb_config.bgw_job where id=:compressjob_id;
\gset
CALL run_job(:compressjob_id);
CALL run_job(:compressjob_id);
select chunk_name, node_name, before_compression_total_bytes, after_compression_total_bytes
from chunk_compression_stats('test_table_int') where compression_status like 'Compressed' order by chunk_name;

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
SELECT recompress_chunk(:'CHUNK_NAME'::regclass);

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
SELECT time_bucket(20, time ), count(*)
FROM test_recomp_int
GROUP BY time_bucket( 20, time) ORDER BY 1;

--check compression_status afterwards--
SELECT recompress_chunk(chunk, true) FROM
( SELECT chunk FROM show_chunks('test_recomp_int') AS chunk ORDER BY chunk LIMIT 2)q;
SELECT * from test_recomp_int_chunk_status ORDER BY 1;

CALL run_job(:compressjob_id);
SELECT * from test_recomp_int_chunk_status ORDER BY 1;

--verify that there are no errors if the policy/recompress_chunk is executed again
--on previously compressed chunks
CALL run_job(:compressjob_id);
SELECT recompress_chunk(chunk, true) FROM
( SELECT chunk FROM show_chunks('test_recomp_int') AS chunk ORDER BY chunk )q;

--decompress and recompress chunk
\set ON_ERROR_STOP 0
SELECT decompress_chunk(chunk, true) FROM
( SELECT chunk FROM show_chunks('test_recomp_int') AS chunk ORDER BY chunk LIMIT 1 )q;
SELECT recompress_chunk(chunk)  FROM
( SELECT chunk FROM show_chunks('test_recomp_int') AS chunk ORDER BY chunk LIMIT 1 )q;
\set ON_ERROR_STOP 1
