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
