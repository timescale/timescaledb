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

SET ROLE :ROLE_DEFAULT_PERM_USER;
-- Test create_chunk_table for errors
\set ON_ERROR_STOP 0
-- Test create_chunk_table for NULL input
SELECT * FROM _timescaledb_internal.create_chunk_table(NULL,' {"time": [1515024000000000, 1519024000000000], "device": [-9223372036854775808, 1073741823]}', '_timescaledb_internal','_hyper_1_1_chunk');
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi', NULL, '_timescaledb_internal','_hyper_1_1_chunk');
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": [1515024000000000, 1519024000000000], "device": [-9223372036854775808, 1073741823]}', NULL,'_hyper_1_1_chunk');
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": [1515024000000000, 1519024000000000], "device": [-9223372036854775808, 1073741823]}', '_timescaledb_internal',NULL);
-- Modified time constraint should fail with collision
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": [1514419600000000, 1515024000000000], "device": [-9223372036854775808, 1073741823]}', '_timescaledb_internal','_hyper_1_1_chunk');
-- Missing dimension
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": [1514419600000000, 1515024000000000]}', '_timescaledb_internal','_hyper_1_1_chunk');
-- Extra dimension
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": [1514419600000000, 1515024000000000],  "device": [-9223372036854775808, 1073741823], "time2": [1514419600000000, 1515024000000000]}', '_timescaledb_internal','_hyper_1_1_chunk');
-- Bad dimension name
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": [1514419600000000, 1515024000000000],  "dev": [-9223372036854775808, 1073741823]}', '_timescaledb_internal','_hyper_1_1_chunk');
-- Same dimension twice
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": [1514419600000000, 1515024000000000], "time": [1514419600000000, 1515024000000000]}', '_timescaledb_internal','_hyper_1_1_chunk');
-- Bad bounds format
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": ["1514419200000000", 1515024000000000], "device": [-9223372036854775808, 1073741823]}', '_timescaledb_internal','_hyper_1_1_chunk');
-- Bad slices format
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": [1515024000000000], "device": [-9223372036854775808, 1073741823]}', '_timescaledb_internal','_hyper_1_1_chunk');
-- Bad slices json
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time: [1515024000000000] "device": [-9223372036854775808, 1073741823]}', '_timescaledb_internal','_hyper_1_1_chunk');
-- Valid chunk, but no permissions
SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT * FROM _timescaledb_internal.create_chunk_table('chunkapi',' {"time": [1515024000000000, 1519024000000000], "device": [-9223372036854775808, 1073741823]}', '_timescaledb_internal','_hyper_1_1_chunk');
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

-- Test create_chunk_table to recreate the chunk table and show dimension slices
SET ROLE :ROLE_DEFAULT_PERM_USER;

SELECT * FROM chunkapi ORDER BY time;

SELECT chunk_schema AS "CHUNK_SCHEMA", chunk_name AS "CHUNK_NAME"
FROM timescaledb_information.chunks c
ORDER BY chunk_name DESC
LIMIT 1 \gset

SELECT slices AS "SLICES"
FROM _timescaledb_internal.show_chunk(:'CHUNK_SCHEMA'||'.'||:'CHUNK_NAME') \gset

SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY id;

SELECT drop_chunks('chunkapi','2018-01-10'::timestamp,'2017-12-23'::timestamp);

SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY id;

SELECT count(*) FROM 
   _timescaledb_internal.create_chunk_table('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');

SELECT * FROM _timescaledb_catalog.dimension_slice ORDER BY id;

-- Test that creat_chunk fails since chunk table already exists
\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_internal.create_chunk('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');
\set ON_ERROR_STOP 1

-- Test create_chunk_table on a hypertable where the chunk didn't exist before

DROP TABLE chunkapi;
CREATE TABLE chunkapi (time timestamptz, device int, temp float);
SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 2);

SELECT count(*) FROM 
   _timescaledb_internal.create_chunk_table('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');

-- Demonstrate that current settings for dimensions don't affect create_chunk_table

DROP TABLE chunkapi;
CREATE TABLE chunkapi (time timestamptz not null, device int, temp float);
SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 2, '3d');

SELECT count(*) FROM 
   _timescaledb_internal.create_chunk_table('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');

DROP TABLE chunkapi;
CREATE TABLE chunkapi (time timestamptz not null, device int, temp float);
SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 3);

SELECT count(*) FROM 
   _timescaledb_internal.create_chunk_table('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');

-- Test create_chunk_table if a colliding chunk exists

DROP TABLE chunkapi;
CREATE TABLE chunkapi (time timestamptz not null, device int, temp float);
SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 3);

INSERT INTO chunkapi VALUES ('2018-01-01 05:00:00-8', 1, 23.4);

\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.create_chunk_table('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');
\set ON_ERROR_STOP 1

-- Test create_chunk_table when a chunk exists in different space partition and thus doesn't collide

DROP TABLE chunkapi;
CREATE TABLE chunkapi (time timestamptz not null, device int, temp float);
SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 2);

INSERT INTO chunkapi VALUES ('2018-01-01 05:00:00-8', 2, 23.4);

SELECT _timescaledb_internal.create_chunk_table('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');

-- Test create_chunk_table when a chunk exists in different time partition and thus doesn't collide

DROP TABLE chunkapi;
CREATE TABLE chunkapi (time timestamptz not null, device int, temp float);
SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 2);

INSERT INTO chunkapi VALUES ('2018-02-01 05:00:00-8', 1, 23.4);

SELECT _timescaledb_internal.create_chunk_table('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');

-- Test create_chunk_table with tablespaces

\c :TEST_DBNAME :ROLE_SUPERUSER
SET client_min_messages = ERROR;
DROP TABLESPACE IF EXISTS tablespace1;
DROP TABLESPACE IF EXISTS tablespace2;
SET client_min_messages = NOTICE;
CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;
CREATE TABLESPACE tablespace2 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE2_PATH;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Use the space partition to calculate the tablespace id to use

DROP TABLE chunkapi;
CREATE TABLE chunkapi (time timestamptz not null, device int, temp float);
SELECT * FROM create_hypertable('chunkapi', 'time', 'device', 3);

SELECT attach_tablespace('tablespace1', 'chunkapi');
SELECT attach_tablespace('tablespace2', 'chunkapi');

SELECT count(*) FROM 
   _timescaledb_internal.create_chunk_table('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');

SELECT tablespace FROM pg_tables WHERE tablename = :'CHUNK_NAME';

DROP TABLE chunkapi;

-- Use the time partition to calculate the tablespace id to use

CREATE TABLE chunkapi (time timestamptz not null, device int, temp float);
SELECT * FROM create_hypertable('chunkapi', 'time');
INSERT INTO chunkapi VALUES ('2018-01-01 05:00:00-8', 1, 23.4);

SELECT chunk_schema AS "CHUNK_SCHEMA", chunk_name AS "CHUNK_NAME"
FROM timescaledb_information.chunks c
ORDER BY chunk_name DESC
LIMIT 1 \gset

SELECT slices AS "SLICES"
FROM _timescaledb_internal.show_chunk(:'CHUNK_SCHEMA'||'.'||:'CHUNK_NAME') \gset

SELECT drop_chunks('chunkapi','2018-01-10'::timestamp,'2017-12-23'::timestamp);

SELECT attach_tablespace('tablespace1', 'chunkapi');
SELECT attach_tablespace('tablespace2', 'chunkapi');

SELECT count(*) FROM 
   _timescaledb_internal.create_chunk_table('chunkapi', :'SLICES', :'CHUNK_SCHEMA', :'CHUNK_NAME');

SELECT tablespace FROM pg_tables WHERE tablename = :'CHUNK_NAME';

DROP TABLE chunkapi;

\c :TEST_DBNAME :ROLE_SUPERUSER
SET client_min_messages = ERROR;
DROP TABLESPACE tablespace1;
DROP TABLESPACE tablespace2;
SET client_min_messages = NOTICE;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
