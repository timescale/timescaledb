-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2

\ir include/remote_exec.sql
GRANT CREATE ON DATABASE :"TEST_DBNAME" TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE SCHEMA "ChunkSchema";
CREATE TABLE chunkapi (time timestamptz, device int, temp float);

SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 2);

INSERT INTO chunkapi VALUES ('2018-01-01 05:00:00-8', 1, 23.4);

SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('chunkapi')
ORDER BY chunk_id;

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
\set ON_ERROR_STOP 1

-- Test that granting insert on tables allow create_chunk to be
-- called. This will also create a chunk that does not collide and has
-- a custom schema and name.
SET ROLE :ROLE_SUPERUSER;
GRANT INSERT ON chunkapi TO :ROLE_DEFAULT_PERM_USER_2;
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi',' {"time": [1515024000000000, 1519024000000000], "device": [-9223372036854775808, 1073741823]}', 'ChunkSchema', 'My_chunk_Table_name');

SET ROLE :ROLE_DEFAULT_PERM_USER;

\set VERBOSITY terse

SELECT (_timescaledb_internal.show_chunk(show_chunks)).*
FROM show_chunks('chunkapi')
ORDER BY chunk_id;

-- Show the new chunks
\dt public.*
\dt "ChunkSchema".*

-- Make ANALYZE deterministic
SELECT setseed(1);

-- Test getting relation stats for chunks.  First get stats
-- chunk-by-chunk. Note that the table isn't ANALYZED, so no stats
-- present yet.
SELECT (_timescaledb_internal.get_chunk_relstats(show_chunks)).*
FROM show_chunks('chunkapi')
ORDER BY chunk_id;
SELECT (_timescaledb_internal.get_chunk_colstats(show_chunks)).*
FROM show_chunks('chunkapi')
ORDER BY chunk_id;

-- Get the same stats but by giving the hypertable as input
SELECT * FROM _timescaledb_internal.get_chunk_relstats('chunkapi');
SELECT * FROM _timescaledb_internal.get_chunk_colstats('chunkapi');

SELECT relname, reltuples, relpages, relallvisible FROM pg_class WHERE relname IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('chunkapi'))
ORDER BY relname;

SELECT tablename, attname, inherited, null_frac, avg_width, n_distinct
FROM pg_stats WHERE tablename IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('chunkapi'))
ORDER BY tablename, attname;

-- Show stats after analyze
ANALYZE chunkapi;
SELECT * FROM _timescaledb_internal.get_chunk_relstats('chunkapi');
SELECT * FROM _timescaledb_internal.get_chunk_colstats('chunkapi');

SELECT relname, reltuples, relpages, relallvisible FROM pg_class WHERE relname IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('chunkapi'))
ORDER BY relname;

SELECT tablename, attname, inherited, null_frac, avg_width, n_distinct
FROM pg_stats WHERE tablename IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
FROM show_chunks('chunkapi'))
ORDER BY tablename, attname;

-- Test getting chunk stats on a distribute hypertable
SET ROLE :ROLE_CLUSTER_SUPERUSER;

SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => :'DN_DBNAME_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => :'DN_DBNAME_2');

GRANT USAGE
   ON FOREIGN SERVER data_node_1, data_node_2
   TO :ROLE_1, :ROLE_DEFAULT_PERM_USER;

SET ROLE :ROLE_1;
CREATE TABLE disttable (time timestamptz, device int, temp float, color text);
SELECT * FROM create_distributed_hypertable('disttable', 'time', 'device');
INSERT INTO disttable VALUES ('2018-01-01 05:00:00-8', 1, 23.4, 'green'),
                             ('2018-01-01 06:00:00-8', 4, 22.3, NULL),
                             ('2018-01-01 06:00:00-8', 1, 21.1, 'green');

-- Make sure we get deterministic behavior across all nodes
CALL distributed_exec($$ SELECT setseed(1); $$);

-- No stats on the local table
SELECT * FROM _timescaledb_internal.get_chunk_relstats('disttable');
SELECT * FROM _timescaledb_internal.get_chunk_colstats('disttable');

SELECT relname, reltuples, relpages, relallvisible FROM pg_class WHERE relname IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('disttable'))
ORDER BY relname;
SELECT * FROM pg_stats WHERE tablename IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('disttable'))
ORDER BY 1,2,3;

-- Run ANALYZE on data node 1
CALL distributed_exec('ANALYZE disttable', '{ "data_node_1" }');

-- Stats should now be refreshed after running get_chunk_{col,rel}stats
SELECT relname, reltuples, relpages, relallvisible FROM pg_class WHERE relname IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('disttable'))
ORDER BY relname;
SELECT * FROM pg_stats WHERE tablename IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('disttable'))
ORDER BY 1,2,3;

SELECT * FROM _timescaledb_internal.get_chunk_relstats('disttable');
SELECT * FROM _timescaledb_internal.get_chunk_colstats('disttable');

SELECT relname, reltuples, relpages, relallvisible FROM pg_class WHERE relname IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('disttable'))
ORDER BY relname;

SELECT * FROM pg_stats WHERE tablename IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('disttable'))
ORDER BY 1,2,3;

-- Test that user without table permissions can't get column stats
SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT * FROM _timescaledb_internal.get_chunk_colstats('disttable');
SET ROLE :ROLE_1;

-- Run ANALYZE again, but on both nodes.
ANALYZE disttable;

-- Now expect stats from all data node chunks
SELECT * FROM _timescaledb_internal.get_chunk_relstats('disttable');
SELECT * FROM _timescaledb_internal.get_chunk_colstats('disttable');

-- Test ANALYZE with a replica chunk. We'd like to ensure the
-- stats-fetching functions handle duplicate stats from different (but
-- identical) replica chunks.
SELECT set_replication_factor('disttable', 2);
INSERT INTO disttable VALUES ('2019-01-01 05:00:00-8', 1, 23.4, 'green');
-- Run twice to test that stats-fetching functions handle replica chunks.
ANALYZE disttable;
ANALYZE disttable;

SELECT relname, reltuples, relpages, relallvisible FROM pg_class WHERE relname IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('disttable'))
ORDER BY relname;
SELECT * FROM pg_stats WHERE tablename IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name
 FROM show_chunks('disttable'))
ORDER BY 1,2,3;

-- Check underlying pg_statistics table (looking at all columns except
-- starelid, which changes depending on how many tests are run before
-- this)
RESET ROLE;
SELECT ch, staattnum, stainherit, stanullfrac, stawidth, stadistinct, stakind1, stakind2, stakind3, stakind4, stakind5, staop1, staop2, staop3, staop4, staop5,
stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5, stavalues1, stavalues2, stavalues3, stavalues4, stavalues5
FROM pg_statistic st, show_chunks('disttable') ch
WHERE st.starelid = ch
ORDER BY ch, staattnum;

SELECT test.remote_exec(NULL, $$
SELECT ch, staattnum, stainherit, stanullfrac, stawidth, stadistinct, stakind1, stakind2, stakind3, stakind4, stakind5, staop1, staop2, staop3, staop4, staop5,
stanumbers1, stanumbers2, stanumbers3, stanumbers4, stanumbers5, stavalues1, stavalues2, stavalues3, stavalues4, stavalues5
FROM pg_statistic st, show_chunks('disttable') ch
WHERE st.starelid = ch
ORDER BY ch, staattnum;
$$);

-- Clean up
RESET ROLE;
TRUNCATE disttable;
TRUNCATE costtable;
SELECT * FROM delete_data_node('data_node_1', force => true);
SELECT * FROM delete_data_node('data_node_2', force => true);
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
