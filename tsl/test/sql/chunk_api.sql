-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
GRANT CREATE ON DATABASE :TEST_DBNAME TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE SCHEMA "ChunkSchema";
CREATE TABLE chunkapi (time timestamptz, device int, temp float);

SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 2);

INSERT INTO chunkapi VALUES ('2018-01-01 05:00:00-8', 1, 23.4);

SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('chunkapi');

-- Creating a chunk with the constraints of an existing chunk should
-- return the existing chunk
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1514419200000000, 1515024000000000], "device": [-9223372036854775808, 1073741823]}');

\set VERBOSITY default
\set ON_ERROR_STOP 0
-- Modified time constraint should fail with collision
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000], "device": [-9223372036854775808, 1073741823]}');
-- Missing dimension
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000]}');
-- Extra dimension
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000],  "device": [-9223372036854775808, 1073741823], "time2": [1514419600000000, 1515024000000000]}');
-- Bad dimension name
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000],  "dev": [-9223372036854775808, 1073741823]}');
-- Same dimension twice
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1514419600000000, 1515024000000000], "time": [1514419600000000, 1515024000000000]}');
-- Bad bounds format
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": ["1514419200000000", 1515024000000000], "device": [-9223372036854775808, 1073741823]}');
-- Bad slices format
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1515024000000000], "device": [-9223372036854775808, 1073741823]}');
-- Bad slices json
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time: [1515024000000000] "device": [-9223372036854775808, 1073741823]}');
-- Valid chunk, but no permissions
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1515024000000000, 1519024000000000], "device": [-9223372036854775808, 1073741823]}', 'ChunkSchema', 'My_chunk_Table_name');
SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 1
\set VERBOSITY terse

-- Create a chunk that does not collide and with custom schema and name
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1515024000000000, 1519024000000000], "device": [-9223372036854775808, 1073741823]}', 'ChunkSchema', 'My_chunk_Table_name');

SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('chunkapi');

-- Show the new chunks
\dt public.*
\dt "ChunkSchema".*


-- Test getting relation stats for chunk.  First get stats
-- chunk-by-chunk. Note that the table isn't ANALYZED, so no stats
-- present yet.
SELECT (_timescaledb_internal.get_chunk_relstats(show_chunks)).*
FROM show_chunks('chunkapi');

-- Get the same stats but by giving the hypertable as input
SELECT * FROM _timescaledb_internal.get_chunk_relstats('chunkapi');

-- Show stats after analyze
ANALYZE chunkapi;
SELECT * FROM _timescaledb_internal.get_chunk_relstats('chunkapi');

-- Test getting chunk stats on a distribute hypertable
SET ROLE :ROLE_CLUSTER_SUPERUSER;

SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
SET client_min_messages TO NOTICE;

SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => 'data_node_2');

GRANT USAGE
   ON FOREIGN SERVER data_node_1, data_node_2
   TO :ROLE_1;

SET ROLE :ROLE_1;
CREATE TABLE disttable (time timestamptz, device int, temp float);
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device');
INSERT INTO disttable VALUES ('2018-01-01 05:00:00-8', 1, 23.4),
                             ('2018-01-01 06:00:00-8', 4, 22.3),
                             ('2018-01-01 06:00:00-8', 1, 21.1);

-- No stats on the local table
SELECT * FROM _timescaledb_internal.get_chunk_relstats('disttable');

-- Run ANALYZE on data node 1
SELECT * FROM distributed_exec('ANALYZE disttable', '{ "data_node_1" }');

-- Stats should now be refreshed locally
SELECT * FROM _timescaledb_internal.get_chunk_relstats('disttable');

-- Run ANALYZE again, but on both nodes
SELECT * FROM distributed_exec('ANALYZE disttable');

-- Now expect stats from all data node chunks
SELECT * FROM _timescaledb_internal.get_chunk_relstats('disttable');

RESET ROLE;
-- Clean up
TRUNCATE disttable;
SELECT * FROM delete_data_node('data_node_1', force => true);
SELECT * FROM delete_data_node('data_node_2', force => true);
DROP DATABASE data_node_1;
DROP DATABASE data_node_2;
