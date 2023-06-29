-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\o
\set ECHO all

\set DATA_NODE_1 :TEST_DBNAME _1
\set DATA_NODE_2 :TEST_DBNAME _2
\set DATA_NODE_3 :TEST_DBNAME _3

SELECT node_name, database, node_created, database_created, extension_created
FROM (
  SELECT (add_data_node(name, host => 'localhost', DATABASE => name)).*
  FROM (VALUES (:'DATA_NODE_1'), (:'DATA_NODE_2'), (:'DATA_NODE_3')) v(name)
) a;

GRANT USAGE ON FOREIGN SERVER :DATA_NODE_1, :DATA_NODE_2, :DATA_NODE_3 TO PUBLIC;
-- though user on access node has required GRANTS, this will propagate GRANTS to the connected data nodes
GRANT CREATE ON SCHEMA public TO :ROLE_1;
SET ROLE :ROLE_1;

CREATE TABLE dist_test(time timestamp NOT NULL, device int, temp float);
SELECT create_distributed_hypertable('dist_test', 'time', 'device', 3);
INSERT INTO dist_test SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;
SELECT * from show_chunks('dist_test');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('dist_test'); $$);

SELECT sum(device) FROM dist_test;
SELECT * FROM test.remote_exec(ARRAY[:'DATA_NODE_1'], $$ SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk; $$);

-- ensure data node name is provided and has proper type
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> null, destination_node => :'DATA_NODE_2');
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => null);
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => 2);
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node => :'DATA_NODE_1');
\set ON_ERROR_STOP 1

-- ensure functions can't be run in read only mode
SET default_transaction_read_only TO on;
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
\set ON_ERROR_STOP 1
SET default_transaction_read_only TO off;

-- ensure functions can't be run in an active multi-statement transaction
\set ON_ERROR_STOP 0
BEGIN;
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
ROLLBACK;
BEGIN;
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
ROLLBACK;
\set ON_ERROR_STOP 1

-- must be superuser to copy/move chunks
SET ROLE :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
\set ON_ERROR_STOP 1
SET ROLE :ROLE_1;

-- can't run copy/move chunk on a data node
\c :DATA_NODE_1 :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- ensure that hypertable chunks are distributed
CREATE TABLE nondist_test(time timestamp NOT NULL, device int, temp float);
SELECT create_hypertable('nondist_test', 'time', 'device', 3);
INSERT INTO nondist_test SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;
SELECT * from show_chunks('nondist_test');
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._hyper_2_5_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._hyper_2_5_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
\set ON_ERROR_STOP 1

-- ensure that chunk exists on a source data node
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_2_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
\set ON_ERROR_STOP 1

-- do actualy copy
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('dist_test'); $$);
SELECT * FROM test.remote_exec(ARRAY[:'DATA_NODE_2'], $$ SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk; $$);

-- ensure that chunk exists on a destination data node
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
\set ON_ERROR_STOP 1

-- now try to move the same chunk from data node 2 to 3
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> :'DATA_NODE_2', destination_node => :'DATA_NODE_3');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('dist_test'); $$);
SELECT * FROM test.remote_exec(ARRAY[:'DATA_NODE_3'], $$ SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk; $$);
SELECT sum(device) FROM dist_test;

-- Check that they can be called from inside a procedure without
-- generating warnings or error messages (#3495).
CREATE OR REPLACE PROCEDURE copy_wrapper(regclass, text, text)
AS $$
BEGIN
    CALL timescaledb_experimental.copy_chunk($1, $2, $3);
END
$$
LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE move_wrapper(regclass, text, text)
AS $$
BEGIN
    CALL timescaledb_experimental.move_chunk($1, $2, $3);
END
$$
LANGUAGE PLPGSQL;

SELECT chunk_name, replica_nodes, non_replica_nodes
FROM timescaledb_experimental.chunk_replication_status;

CALL copy_wrapper('_timescaledb_internal._dist_hyper_1_3_chunk', :'DATA_NODE_3', :'DATA_NODE_2');
CALL move_wrapper('_timescaledb_internal._dist_hyper_1_2_chunk', :'DATA_NODE_2', :'DATA_NODE_1');

SELECT chunk_name, replica_nodes, non_replica_nodes
FROM timescaledb_experimental.chunk_replication_status;

DROP PROCEDURE copy_wrapper;
DROP PROCEDURE move_wrapper;

DROP TABLE dist_test;

-- Test copy/move compressed chunk
--

-- Create a compressed hypertable
CREATE TABLE dist_test(time timestamp NOT NULL, device int, temp float);
SELECT create_distributed_hypertable('dist_test', 'time', 'device', 3);
INSERT INTO dist_test SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;
ALTER TABLE dist_test SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby = 'time DESC');

-- Integrity check (see below)
SELECT sum(device) FROM dist_test;

-- Get a list of chunks
SELECT * from show_chunks('dist_test');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('dist_test'); $$);

SELECT chunk_schema || '.' ||  chunk_name, data_nodes
FROM timescaledb_information.chunks
WHERE hypertable_name = 'dist_test';

-- Compress a chunk
SELECT compress_chunk('_timescaledb_internal._dist_hyper_3_12_chunk');
SELECT * FROM _timescaledb_internal._dist_hyper_3_12_chunk ORDER BY time;

-- Get compressed chunk name on the source data node and show its content
\c :DATA_NODE_1 :ROLE_CLUSTER_SUPERUSER;

SELECT c2.table_name
FROM _timescaledb_catalog.chunk c1
JOIN _timescaledb_catalog.chunk c2 ON (c1.compressed_chunk_id = c2.id)
WHERE c1.table_name = '_dist_hyper_3_12_chunk';

SELECT * FROM _timescaledb_internal.compress_hyper_3_7_chunk ORDER BY device, _ts_meta_min_1;
SELECT * FROM _timescaledb_internal._dist_hyper_3_12_chunk ORDER BY time;

-- Get compressed chunk stat
SELECT * FROM _timescaledb_internal.compressed_chunk_stats WHERE  chunk_name = '_dist_hyper_3_12_chunk';

-- Move compressed chunk from data node 1 to data node 2
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_3_12_chunk', source_node=> :'DATA_NODE_1', destination_node => :'DATA_NODE_2');
SELECT count(*) FROM  _timescaledb_catalog.chunk_copy_operation;

-- Make sure same compressed chunk hash been created on the destination data node
\c :DATA_NODE_2 :ROLE_CLUSTER_SUPERUSER;

-- Chunk created on data node has different id but the same name, make sure
-- compressed_chunk_id is correctly set
SELECT c2.table_name
FROM _timescaledb_catalog.chunk c1
JOIN _timescaledb_catalog.chunk c2 ON (c1.compressed_chunk_id = c2.id)
WHERE c1.table_name = '_dist_hyper_3_12_chunk';

-- Try to query hypertable member with compressed chunk
SELECT * FROM _timescaledb_internal.compress_hyper_3_7_chunk ORDER BY device, _ts_meta_min_1;
SELECT * FROM _timescaledb_internal._dist_hyper_3_12_chunk ORDER BY time;

-- Ensure that compressed chunk stats match stats from the source data node
SELECT * FROM _timescaledb_internal.compressed_chunk_stats WHERE chunk_name = '_dist_hyper_3_12_chunk';

-- Ensure moved chunks are no longer present on the source data node
\c :DATA_NODE_1 :ROLE_CLUSTER_SUPERUSER;

SELECT c2.table_name
FROM _timescaledb_catalog.chunk c1
JOIN _timescaledb_catalog.chunk c2 ON (c1.compressed_chunk_id = c2.id)
WHERE c1.table_name = '_dist_hyper_3_12_chunk';

\set ON_ERROR_STOP 0
SELECT * FROM _timescaledb_internal.compress_hyper_3_7_chunk ORDER BY device, _ts_meta_min_1;
SELECT * FROM _timescaledb_internal._dist_hyper_3_12_chunk ORDER BY time;
\set ON_ERROR_STOP 1

-- Make sure chunk has been properly moved from AN
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

SELECT * FROM show_chunks('dist_test');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('dist_test'); $$);

SELECT chunk_schema || '.' ||  chunk_name, data_nodes
FROM timescaledb_information.chunks
WHERE hypertable_name = 'dist_test';

-- Query distributed hypertable again to query newly moved chunk, make
-- sure result has not changed
SELECT sum(device) FROM dist_test;

-- Test operation_id name validation

\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(operation_id => ' move chunk id ', chunk=>'_timescaledb_internal._dist_hyper_3_12_chunk', source_node=> :'DATA_NODE_2', destination_node => :'DATA_NODE_3');
CALL timescaledb_experimental.move_chunk(operation_id => 'ChUnK_MoVe_Op', chunk=>'_timescaledb_internal._dist_hyper_3_12_chunk', source_node=> :'DATA_NODE_2', destination_node => :'DATA_NODE_3');
CALL timescaledb_experimental.move_chunk(operation_id => '_ID123', chunk=>'_timescaledb_internal._dist_hyper_3_12_chunk', source_node=> :'DATA_NODE_2', destination_node => :'DATA_NODE_3');
\set ON_ERROR_STOP 1

-- Now copy chunk from data node 2 to data node 3
CALL timescaledb_experimental.move_chunk(operation_id => 'id123', chunk=>'_timescaledb_internal._dist_hyper_3_12_chunk', source_node=> :'DATA_NODE_2', destination_node => :'DATA_NODE_3');

\c :DATA_NODE_3 :ROLE_CLUSTER_SUPERUSER;

-- Validate chunk on data node 3
SELECT c2.table_name
FROM _timescaledb_catalog.chunk c1
JOIN _timescaledb_catalog.chunk c2 ON (c1.compressed_chunk_id = c2.id)
WHERE c1.table_name = '_dist_hyper_3_12_chunk';

SELECT * FROM _timescaledb_internal.compress_hyper_3_7_chunk ORDER BY device, _ts_meta_min_1;
SELECT * FROM _timescaledb_internal._dist_hyper_3_12_chunk ORDER BY time;
SELECT * FROM _timescaledb_internal.compressed_chunk_stats WHERE chunk_name = '_dist_hyper_3_12_chunk';

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- _dist_hyper_3_12_chunk should be moved in data node 3 now
SELECT chunk_schema || '.' ||  chunk_name, data_nodes
FROM timescaledb_information.chunks
WHERE hypertable_name = 'dist_test';

RESET ROLE;
DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;
