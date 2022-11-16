-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- This file contains tests for all features that will be used as part
-- of the chunk move/copy multi-node functionality


-- A table for the first chunk will be created on the data node, where it is not present.
SELECT chunk_name, data_nodes
FROM timescaledb_information.chunks
WHERE hypertable_name = 'dist_chunk_copy'
ORDER BY 1, 2;

-- Non-distributed chunk will be used to test an error
SELECT chunk_name
FROM timescaledb_information.chunks
WHERE hypertable_name = 'conditions'
ORDER BY 1;

-- Test function _timescaledb_internal.create_chunk_replica_table
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.create_chunk_replica_table(NULL, 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', NULL);
SELECT _timescaledb_internal.create_chunk_replica_table(1234, 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('metrics_int', 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('conditions', 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._hyper_10_51_chunk', 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_2');
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_27_chunk', 'data_node_2');
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_4');
BEGIN READ ONLY;
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_3');
COMMIT;
\set ON_ERROR_STOP 1

SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_1');

-- Test that the table cannot be created since it was already created on the data node
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_3');
\set ON_ERROR_STOP 1

-- Creating chunk replica table ignores compression now:
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_70_chunk', 'data_node_2');

CALL distributed_exec($$ DROP TABLE _timescaledb_internal._dist_hyper_15_67_chunk $$, '{"data_node_1"}');
CALL distributed_exec($$ DROP TABLE _timescaledb_internal._dist_hyper_15_70_chunk $$, '{"data_node_2"}');

-- Test function _timescaledb_internal.chunk_drop_replica
-- Sanity checking of the chunk_drop_replica API

\set ON_ERROR_STOP 0
-- Check that it doesn't work in a read only transaction
SET default_transaction_read_only TO on;
SELECT _timescaledb_internal.chunk_drop_replica(NULL, NULL);
RESET default_transaction_read_only;

-- NULL input for chunk id errors out
SELECT _timescaledb_internal.chunk_drop_replica(NULL, NULL);

-- Specifying any regular hypertable instead of chunk errors out
SELECT _timescaledb_internal.chunk_drop_replica('public.metrics', NULL);

-- Specifying regular hypertable chunk on a proper data node errors out
SELECT _timescaledb_internal.chunk_drop_replica('_timescaledb_internal._hyper_1_1_chunk', 'data_node_1');

-- Specifying non-existent chunk on a proper data node errors out
SELECT _timescaledb_internal.chunk_drop_replica('_timescaledb_internal._dist_hyper_700_38_chunk', 'data_node_1');

-- Get the last chunk for this hypertable
SELECT ch1.schema_name|| '.' || ch1.table_name as "CHUNK_NAME", ch1.id "CHUNK_ID"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id
AND ht.table_name = 'mvcp_hyper'
ORDER BY ch1.id DESC LIMIT 1 \gset

-- Specifying wrong node name errors out
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', 'bad_node');

-- This chunk contains only one entry as of now
SELECT * FROM :CHUNK_NAME;

-- Specifying NULL node name along with proper chunk errors out
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', NULL);

\set ON_ERROR_STOP 1
-- Check the current primary foreign server for this chunk, that will change
-- post the chunk_drop_replica call
SELECT foreign_server_name AS "PRIMARY_CHUNK_NODE"
    FROM information_schema.foreign_tables WHERE
    foreign_table_name = split_part(:'CHUNK_NAME', '.', 2)
    ORDER BY 1 \gset

-- Show the node that was primary for the chunk
\echo :PRIMARY_CHUNK_NODE

-- Drop the chunk replica on the primary chunk node. Should succeed
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', :'PRIMARY_CHUNK_NODE');

-- The primary foreign server for the chunk should be updated now
SELECT foreign_server_name
    FROM information_schema.foreign_tables WHERE
    foreign_table_name = split_part(:'CHUNK_NAME', '.', 2)
    ORDER BY 1;

-- Number of replicas should have been reduced by 1
SELECT count(*) FROM _timescaledb_catalog.chunk_data_node WHERE chunk_id = :'CHUNK_ID';

-- Ensure that INSERTs still work on this mvcp_hyper table into this chunk
-- Rollback to not modify the shared test state
BEGIN;
INSERT INTO mvcp_hyper VALUES (1001, 1001);
-- Ensure that SELECTs are able to query data from the above chunk
SELECT count(*) FROM mvcp_hyper WHERE time >= 1000;
ROLLBACK;


-- Check that chunk_drop_replica works with compressed chunk
SELECT substr(compress_chunk(:'CHUNK_NAME')::TEXT, 1, 29);

-- Drop one replica of a valid chunk. Should succeed on another datanode
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', 'data_node_2');

-- Number of replicas should have been reduced by 1
SELECT count(*) FROM _timescaledb_catalog.chunk_data_node WHERE chunk_id = :'CHUNK_ID';

-- Decompress before checking INSERTs
SELECT substr(decompress_chunk(:'CHUNK_NAME')::TEXT, 1, 29);

-- Ensure that INSERTs still work on this mvcp_hyper table into this chunk
-- Rollback to not modify the shared test state
BEGIN;
INSERT INTO mvcp_hyper VALUES (1002, 1002);
-- Ensure that SELECTs are able to query data from the above chunk
SELECT count(*) FROM mvcp_hyper WHERE time >= 1000;
ROLLBACK;

-- Drop one replica of a valid chunk. Should not succeed on last datanode
SELECT foreign_server_name AS "PRIMARY_CHUNK_NODE"
    FROM information_schema.foreign_tables WHERE
    foreign_table_name = split_part(:'CHUNK_NAME', '.', 2)
    ORDER BY 1 \gset

\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', :'PRIMARY_CHUNK_NODE');
\set ON_ERROR_STOP 1
