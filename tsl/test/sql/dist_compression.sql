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

SELECT * FROM timescaledb_information.compression_settings;
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
WHERE hypertable_name = 'compressed';
SELECT * from timescaledb_information.chunks 
ORDER BY hypertable_name, chunk_name;
SELECT * from timescaledb_information.dimensions 
ORDER BY hypertable_name, dimension_number;
\x

SELECT * FROM chunks_detailed_size('compressed'::regclass) 
ORDER BY chunk_name, node_name;
SELECT * FROM hypertable_detailed_size('compressed'::regclass) ORDER BY node_name;

-- Test compression policy with distributed hypertable
--
\set ON_ERROR_STOP 0
SELECT add_compression_policy('compressed', '60d'::interval);
\set ON_ERROR_STOP 1

-- Disable compression on distributed table tests
ALTER TABLE compressed SET (timescaledb.compress = false);

SELECT table_name, compression_state, compressed_hypertable_id
FROM _timescaledb_catalog.hypertable
ORDER BY 1;

SELECT * FROM timescaledb_information.compression_settings;

--Now re-enable compression
ALTER TABLE compressed SET (timescaledb.compress);
SELECT table_name, compression_state, compressed_hypertable_id
FROM _timescaledb_catalog.hypertable
ORDER BY 1;
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(chunk, if_not_compressed => true)
FROM show_chunks('compressed') AS chunk
ORDER BY chunk
LIMIT 1;

SELECT chunk_name, node_name, compression_status
FROM chunk_compression_stats('compressed')
ORDER BY 1, 2; 
