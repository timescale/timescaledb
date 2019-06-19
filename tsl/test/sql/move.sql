-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ON_ERROR_STOP 0

\c :TEST_DBNAME :ROLE_SUPERUSER
SET client_min_messages = ERROR;
DROP TABLESPACE IF EXISTS tablespace1;
DROP TABLESPACE IF EXISTS tablespace2;
DROP TABLESPACE IF EXISTS tablespace3;
SET client_min_messages = NOTICE;

CREATE TABLESPACE tablespace1 OWNER :ROLE_DEFAULT_PERM_USER LOCATION :TEST_TABLESPACE1_PATH;
CREATE TABLESPACE tablespace2 OWNER :ROLE_DEFAULT_PERM_USER_2 LOCATION :TEST_TABLESPACE2_PATH;

--Running some of the same tests as we do for reorder/cluster because that's how move is implemented
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

\set ON_ERROR_STOP 1

\ir include/cluster_test_setup.sql

BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;

-- Show chunk indexes
SELECT * FROM test.show_indexes('_timescaledb_internal._hyper_1_1_chunk');
SELECT * FROM test.show_indexes('_timescaledb_internal._hyper_1_2_chunk');

\set ON_ERROR_STOP 0
-- cannot move a chunk with no reorder index on first call 
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace1');
-- cannot move a chunk without a destination tablespace set
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>NULL, index_destination_tablespace=>'tablespace1', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx');
-- cannot move a chunk without an index_destination_tablespace set
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace1', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx');
-- cannot move a chunk or an index to a tablespace we do not have create permissions on 
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace2', index_destination_tablespace=>'tablespace1', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx');
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace2', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx');
-- cannot move to a nonexistent tablespace 
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace3', index_destination_tablespace=>'tablespace1', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx');
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace3', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx');
-- cannot move a hypertable (must specify a chunk)
SELECT move_chunk(chunk=>'cluster_test', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace2', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx');
-- cannot move a NULL chunk
SELECT move_chunk(chunk=>NULL, destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace2', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx');
-- cannot move within a transaction
BEGIN;
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace2', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx');
END;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER_2 
-- must be hypertable owner to move a chunk
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace2', index_destination_tablespace=>'tablespace2', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx', verbose=>TRUE);
\set ON_ERROR_STOP 1

-- grant create permissions on tablespace2 so we can use it later
GRANT CREATE ON TABLESPACE tablespace2 TO :ROLE_DEFAULT_PERM_USER;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- move with chunk index for reorder
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace1', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx', verbose=>TRUE);
SELECT * FROM test.show_subtables('cluster_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');
BEGIN;
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', seqscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', indexscan => true) \gexec
SELECT * FROM ensure_scans_work('cluster_test', should_output => 'expected', bitmapscan => true) \gexec
COMMIT;
SET enable_seqscan=Default;
SET enable_indexscan=Default;
SET enable_bitmapscan=Default;
-- check that move puts things in the correct order, as it also reorders
SELECT ctid, time, temp, location, substring(value for 30), length(value)
FROM _timescaledb_internal._hyper_1_1_chunk ORDER BY ctid;
SELECT ctid, time, temp, location, substring(value for 30), length(value)
FROM _timescaledb_internal._hyper_1_2_chunk ORDER BY ctid;

-- move back to pg_default
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'pg_default', index_destination_tablespace=>'pg_default', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx', verbose=>TRUE);
SELECT * FROM test.show_subtables('cluster_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');


-- move chunk and indexes to different tablespaces
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'tablespace1', index_destination_tablespace=>'tablespace2', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx', verbose=>TRUE);
SELECT * FROM test.show_subtables('cluster_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');


-- keep chunk in same space and move index tablespace
SELECT move_chunk(chunk=>'_timescaledb_internal._hyper_1_2_chunk', destination_tablespace=>'pg_default', index_destination_tablespace=>'tablespace2', reorder_index=>'_timescaledb_internal._hyper_1_2_chunk_cluster_test_time_idx', verbose=>TRUE);
SELECT * FROM test.show_subtables('cluster_test');
SELECT * FROM test.show_indexesp('_timescaledb_internal._hyper%_chunk');
