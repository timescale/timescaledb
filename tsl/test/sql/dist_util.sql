-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

----------------------------------------------------------------
-- Test version compability function
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

CREATE OR REPLACE FUNCTION compatible_version(version CSTRING, reference CSTRING)
RETURNS TABLE(is_compatible BOOLEAN, is_old_version BOOLEAN)
AS :TSL_MODULE_PATHNAME, 'ts_test_compatible_version'
LANGUAGE C VOLATILE;

SELECT * FROM compatible_version('2.0.0-beta3.19', reference => '2.0.0-beta3.19');
SELECT * FROM compatible_version('2.0.0', reference => '2.0.0');
SELECT * FROM compatible_version('1.9.9', reference => '2.0.0-beta3.19');
SELECT * FROM compatible_version('1.9.9', reference => '2.0.0');
SELECT * FROM compatible_version('2.0.9', reference => '2.0.0-beta3.19');
SELECT * FROM compatible_version('2.0.9', reference => '2.0.0');
SELECT * FROM compatible_version('2.1.9', reference => '2.0.0-beta3.19');
SELECT * FROM compatible_version('2.1.0', reference => '2.1.19-beta3.19');

-- These should not parse and instead generate an error.
\set ON_ERROR_STOP 0
SELECT * FROM compatible_version('2.1.*', reference => '2.1.19-beta3.19');
SELECT * FROM compatible_version('2.1.0', reference => '2.1.*');
\set ON_ERROR_STOP 1

----------------------------------------------------------------
-- Create two distributed databases

CREATE DATABASE frontend_1;
CREATE DATABASE frontend_2;

\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
UPDATE _timescaledb_catalog.metadata SET value = '87c235e9-d857-4f16-b59f-7fbac9b87664' WHERE key = 'uuid';
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE '%uuid';
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => 'backend_1_1');
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE '%uuid';
SET client_min_messages TO NOTICE;

-- Create a second frontend database and add a backend to it
\c frontend_2 :ROLE_CLUSTER_SUPERUSER
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
UPDATE _timescaledb_catalog.metadata SET value = '77348176-09da-4a80-bc78-e31bdf5e63ec' WHERE key = 'uuid';
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE '%uuid';
SELECT * FROM add_data_node('data_node_1', host => 'localhost', database => 'backend_2_1');
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE '%uuid';
SET client_min_messages TO NOTICE;

\set ON_ERROR_STOP 0

----------------------------------------------------------------
-- Adding frontend as backend to a different frontend should fail
\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'frontend_2', bootstrap => true);
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'frontend_2', bootstrap => false);

----------------------------------------------------------------
-- Adding backend from a different group as a backend should fail
\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'backend_2_1', bootstrap => true);
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'backend_2_1', bootstrap => false);

----------------------------------------------------------------
-- Adding a valid backend target but to an existing backend should fail
\c backend_1_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'backend_2_1', bootstrap => true);
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'backend_2_1', bootstrap => false);

----------------------------------------------------------------
-- Adding a frontend (frontend 1) as a backend to a nondistributed node (TEST_DBNAME) should fail
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'frontend_1', bootstrap => true);
SELECT * FROM add_data_node('invalid_data_node', host => 'localhost', database => 'frontend_1', bootstrap => false);

\set ON_ERROR_STOP 1

----------------------------------------------------------------
-- Test that a data node can be moved to a different frontend if it is
-- removed first.
\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => 'backend_x_2', bootstrap => true);

-- dist_uuid should be added to the metadata on the data node
\c backend_x_2 :ROLE_CLUSTER_SUPERUSER
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';

-- Now remove a backend from this distributed database to add it to the other cluster
\c frontend_1 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM delete_data_node('data_node_2');

-- dist_uuid should not be removed from the metadata on the data node,
-- so we need to delete it manually before adding it to another
-- backend.
\c backend_x_2 :ROLE_CLUSTER_SUPERUSER
SELECT key FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid';
DELETE FROM _timescaledb_catalog.metadata WHERE key = 'dist_uuid';

-- Add the data node to the second frontend without bootstrapping
\c frontend_2 :ROLE_CLUSTER_SUPERUSER
SELECT * FROM add_data_node('data_node_2', host => 'localhost', database => 'backend_x_2', bootstrap => false);

-- dist_uuid should be added to the metadata on the data node
\c backend_x_2 :ROLE_CLUSTER_SUPERUSER
SELECT key, value FROM _timescaledb_catalog.metadata WHERE key LIKE 'dist_uuid';

-- Test space reporting functions for distributed and non-distributed tables
\c frontend_2 :ROLE_CLUSTER_SUPERUSER
CREATE TABLE nondisttable(time timestamptz, device int CHECK (device > 0), temp float);
CREATE TABLE disttable(time timestamptz, device int CHECK (device > 0), temp float);
SELECT * FROM create_hypertable('nondisttable', 'time', create_default_indexes => false);
SELECT * FROM create_distributed_hypertable('disttable', 'time', create_default_indexes => false);

SELECT node_name FROM timescaledb_information.data_nodes
ORDER BY node_name;
SELECT * FROM timescaledb_information.hypertables
ORDER BY hypertable_schema, hypertable_name;

-- Test size functions on empty distributed hypertable.
--
-- First, show the output from standard PG size functions. The
-- functions are expected to remove 0 table bytes for the distributed
-- hypertable since it doesn't have local storage.
SELECT pg_table_size('disttable'), pg_relation_size('disttable'), pg_indexes_size('disttable'), pg_total_relation_size('disttable');
SELECT pg_table_size(ch), pg_relation_size(ch), pg_indexes_size(ch), pg_total_relation_size(ch)
FROM show_chunks('disttable') ch;
SELECT * FROM _timescaledb_internal.relation_size('disttable');
SELECT * FROM show_chunks('disttable') ch JOIN LATERAL _timescaledb_internal.relation_size(ch) ON TRUE;
SELECT pg_table_size('nondisttable'), pg_relation_size('nondisttable'), pg_indexes_size('nondisttable'), pg_total_relation_size('nondisttable');
SELECT pg_table_size(ch), pg_relation_size(ch), pg_indexes_size(ch), pg_total_relation_size(ch)
FROM show_chunks('nondisttable') ch;
SELECT * FROM _timescaledb_internal.relation_size('nondisttable');
SELECT * FROM show_chunks('nondisttable') ch JOIN LATERAL _timescaledb_internal.relation_size(ch) ON TRUE;

SELECT * FROM hypertable_size('disttable');
SELECT * FROM hypertable_size('nondisttable');
SELECT * FROM hypertable_detailed_size('disttable') ORDER BY node_name;
SELECT * FROM hypertable_detailed_size('nondisttable') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunks_detailed_size('nondisttable') ORDER BY  chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_compression_stats('disttable') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('nondisttable') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunk_compression_stats('nondisttable') ORDER BY  chunk_schema, chunk_name, node_name;

-- Create primary key index and check how it affects the size of the
-- empty hypertables.
ALTER TABLE nondisttable ADD CONSTRAINT nondisttable_pkey PRIMARY KEY (time);
ALTER TABLE disttable ADD CONSTRAINT disttable_pkey PRIMARY KEY (time);

SELECT pg_table_size('disttable'), pg_relation_size('disttable'), pg_indexes_size('disttable'), pg_total_relation_size('disttable');
SELECT * FROM _timescaledb_internal.relation_size('disttable');
SELECT pg_table_size('nondisttable'), pg_relation_size('nondisttable'), pg_indexes_size('nondisttable'), pg_total_relation_size('nondisttable');
SELECT * FROM _timescaledb_internal.relation_size('nondisttable');

-- Note that the empty disttable is three times the size of the
-- nondisttable since it has primary key indexes on two data nodes in
-- addition to the access node.
SELECT * FROM hypertable_size('disttable');
SELECT * FROM hypertable_size('nondisttable');
SELECT * FROM hypertable_detailed_size('disttable') ORDER BY node_name;
SELECT * FROM hypertable_detailed_size('nondisttable') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunks_detailed_size('nondisttable') ORDER BY  chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_compression_stats('disttable') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('nondisttable') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunk_compression_stats('nondisttable') ORDER BY  chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_index_size('disttable_pkey');
SELECT * FROM hypertable_index_size('nondisttable_pkey');


-- Test size functions on tables with an empty chunk
INSERT INTO nondisttable VALUES ('2017-01-01 06:01', 1, 1.1);
INSERT INTO disttable SELECT * FROM nondisttable;

SELECT pg_table_size('disttable'), pg_relation_size('disttable'), pg_indexes_size('disttable'), pg_total_relation_size('disttable');
SELECT pg_table_size(ch), pg_relation_size(ch), pg_indexes_size(ch), pg_total_relation_size(ch)
FROM show_chunks('disttable') ch;
SELECT * FROM _timescaledb_internal.relation_size('disttable');
SELECT * FROM show_chunks('disttable') ch JOIN LATERAL _timescaledb_internal.relation_size(ch) ON TRUE;
SELECT pg_table_size('nondisttable'), pg_relation_size('nondisttable'), pg_indexes_size('nondisttable'), pg_total_relation_size('nondisttable');
SELECT pg_table_size(ch), pg_relation_size(ch), pg_indexes_size(ch), pg_total_relation_size(ch)
FROM show_chunks('nondisttable') ch;
SELECT * FROM _timescaledb_internal.relation_size('nondisttable');
SELECT * FROM show_chunks('nondisttable') ch JOIN LATERAL _timescaledb_internal.relation_size(ch) ON TRUE;

SELECT * FROM hypertable_size('disttable');
SELECT * FROM hypertable_detailed_size('disttable') ORDER BY node_name;
SELECT * FROM hypertable_size('nondisttable');
SELECT * FROM hypertable_detailed_size('nondisttable') ORDER BY node_name;

-- Delete all data, but keep chunks
DELETE FROM nondisttable;
DELETE FROM disttable;
VACUUM FULL ANALYZE nondisttable;
VACUUM FULL ANALYZE disttable;

SELECT pg_table_size('disttable'), pg_relation_size('disttable'), pg_indexes_size('disttable'), pg_total_relation_size('disttable');
SELECT pg_table_size(ch), pg_relation_size(ch), pg_indexes_size(ch)
FROM show_chunks('disttable') ch;
SELECT * FROM _timescaledb_internal.relation_size('disttable');
SELECT * FROM show_chunks('disttable') ch JOIN LATERAL _timescaledb_internal.relation_size(ch) ON TRUE;
SELECT pg_table_size('nondisttable'), pg_relation_size('nondisttable'), pg_indexes_size('nondisttable'), pg_total_relation_size('nondisttable');
SELECT pg_table_size(ch), pg_relation_size(ch), pg_indexes_size(ch), pg_total_relation_size(ch)
FROM show_chunks('nondisttable') ch;
SELECT * FROM _timescaledb_internal.relation_size('nondisttable');
SELECT * FROM show_chunks('nondisttable') ch JOIN LATERAL _timescaledb_internal.relation_size(ch) ON TRUE;

SELECT * FROM hypertable_size('disttable');
SELECT * FROM hypertable_detailed_size('disttable') ORDER BY node_name;
SELECT * FROM hypertable_size('nondisttable');
SELECT * FROM hypertable_detailed_size('nondisttable') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunks_detailed_size('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_compression_stats('disttable') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('nondisttable') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunk_compression_stats('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_index_size('disttable_pkey');
SELECT * FROM hypertable_index_size('nondisttable_pkey');

-- Test size functions on non-empty hypertable
INSERT INTO nondisttable VALUES
       ('2017-01-01 06:01', 1, 1.1),
       ('2017-01-01 08:01', 1, 1.2),
       ('2018-01-02 08:01', 2, 1.3),
       ('2019-01-01 09:11', 3, 2.1),
       ('2017-01-01 06:05', 1, 1.4);
INSERT INTO disttable SELECT * FROM nondisttable;

SELECT * FROM hypertable_size('disttable');
SELECT * FROM hypertable_size('nondisttable');
SELECT * FROM hypertable_detailed_size('disttable') ORDER BY node_name;
SELECT * FROM hypertable_detailed_size('nondisttable') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunks_detailed_size('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_compression_stats('disttable') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('nondisttable') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunk_compression_stats('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_index_size('disttable_pkey');
SELECT * FROM hypertable_index_size('nondisttable_pkey');

-- Enable compression
ALTER TABLE nondisttable
SET (timescaledb.compress,
	 timescaledb.compress_segmentby='device',
	 timescaledb.compress_orderby = 'time DESC');

ALTER TABLE disttable
SET (timescaledb.compress,
	 timescaledb.compress_segmentby='device',
	 timescaledb.compress_orderby = 'time DESC');

SELECT * FROM hypertable_size('disttable');
SELECT * FROM hypertable_size('nondisttable');
SELECT * FROM hypertable_detailed_size('disttable') ORDER BY node_name;
SELECT * FROM hypertable_detailed_size('nondisttable') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunks_detailed_size('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_compression_stats('disttable') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('nondisttable') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunk_compression_stats('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_index_size('disttable_pkey');
SELECT * FROM hypertable_index_size('nondisttable_pkey');

-- Compress two chunks (out of three) to see effect of compression
SELECT compress_chunk(ch)
FROM show_chunks('disttable') ch
LIMIT 2;

SELECT compress_chunk(ch)
FROM show_chunks('nondisttable') ch
LIMIT 2;

SELECT * FROM hypertable_size('disttable');
SELECT * FROM hypertable_size('nondisttable');
SELECT * FROM hypertable_detailed_size('disttable') ORDER BY node_name;
SELECT * FROM hypertable_detailed_size('nondisttable') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunks_detailed_size('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_compression_stats('disttable') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('nondisttable') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunk_compression_stats('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_index_size('disttable_pkey');
SELECT * FROM hypertable_index_size('nondisttable_pkey');

-- Make sure functions work for non-superuser
CREATE TABLE size_test_table (value int);
INSERT INTO size_test_table SELECT * FROM generate_series(0, 10000);

SET ROLE :ROLE_1;

-- No query permissions
\set ON_ERROR_STOP 0
SELECT count(*) FROM disttable;
SELECT count(*) FROM size_test_table;
\set ON_ERROR_STOP 1

-- Size functions work anyway, similar to pg_table_size, et al.
-- pg_table_size() can vary with platform so not outputting
SELECT 1 FROM pg_table_size('size_test_table');
SELECT 1 FROM pg_table_size('disttable');
SELECT 1 FROM pg_table_size('nondisttable');
SELECT * FROM hypertable_size('disttable');
SELECT * FROM hypertable_size('nondisttable');
SELECT * FROM hypertable_detailed_size('disttable') ORDER BY node_name;
SELECT * FROM hypertable_detailed_size('nondisttable') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunks_detailed_size('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_compression_stats('disttable') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('nondisttable') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('disttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM chunk_compression_stats('nondisttable') ORDER BY chunk_schema, chunk_name, node_name;
SELECT * FROM hypertable_index_size('disttable_pkey');
SELECT * FROM hypertable_index_size('nondisttable_pkey');

RESET ROLE;
GRANT SELECT ON disttable TO :ROLE_1;
SET ROLE :ROLE_1;
-- Querying should now work
SELECT count(*) FROM disttable;

-- Make sure timescaledb.ssl_dir and passfile gucs can be read by a non-superuser
\c :TEST_DBNAME :ROLE_1
\unset ECHO
\o /dev/null
SHOW timescaledb.ssl_dir;
SHOW timescaledb.passfile;
\o
\set ECHO all
\set ON_ERROR_STOP 0
SET timescaledb.ssl_dir TO 'ssldir';
SET timescaledb.passfile TO 'passfile';
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
SET client_min_messages TO ERROR;
DROP DATABASE backend_1_1;
DROP DATABASE backend_x_2;
DROP DATABASE backend_2_1;
DROP DATABASE frontend_1;
DROP DATABASE frontend_2;
