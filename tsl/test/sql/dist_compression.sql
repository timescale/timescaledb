-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

---------------------------------------------------
-- Test compression on a distributed hypertable
---------------------------------------------------
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
DROP DATABASE IF EXISTS data_node_3;
\ir include/remote_exec.sql

SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => 'data_node_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost',
                            database => 'data_node_3');

GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO :ROLE_1;
SET client_min_messages TO NOTICE;
SET ROLE :ROLE_1;

CREATE TABLE compressed(time timestamptz, device int, temp float);
-- Replicate twice to see that compress_chunk compresses all replica chunks
SELECT create_distributed_hypertable('compressed', 'time', 'device', replication_factor => 2);
INSERT INTO compressed SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, random()*80
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-04 1:00', '1 hour') t;
ALTER TABLE compressed SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby = 'time DESC');

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

-- Compress twice to generate NOTICE that the chunk is already compressed
SELECT compress_chunk(chunk, if_not_compressed => true)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk
LIMIT 1;

-- Decompress the chunk and replica
SELECT decompress_chunk(chunk)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk
LIMIT 1;

-- Should now be decompressed
SELECT * from chunk_compression_stats( 'compressed')
ORDER BY chunk_name, node_name;

-- Decompress twice to generate NOTICE that the chunk is already decompressed
SELECT decompress_chunk(chunk, if_compressed => true)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk
LIMIT 1;

\x
SELECT * FROM timescaledb_information.hypertables
WHERE table_name = 'compressed';
SELECT * from timescaledb_information.chunks 
ORDER BY hypertable_name, chunk_name;
SELECT * from timescaledb_information.dimensions 
ORDER BY hypertable_name, dimension_number;
\x

SELECT * FROM chunks_detailed_size('compressed'::regclass) 
ORDER BY chunk_name, node_name;
SELECT * FROM hypertable_detailed_size('compressed'::regclass);

------------------------------------------------------
-- Test compression policy on a distributed hypertable
------------------------------------------------------

INSERT INTO compressed VALUES 
(now()::TIMESTAMPTZ, 1, 0.1), (now()::TIMESTAMPTZ, 2, 0.2);

\set ON_ERROR_STOP 0
SELECT add_compression_policy('compressed', '60d'::interval);
\set ON_ERROR_STOP 1
