-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test function _timescaledb_internal.create_chunk_replica_table

-- A table for the first chunk will be created on the data node, where it is not present.
SELECT chunk_name, data_nodes 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'dist_chunk_copy';

SELECT compress_chunk('_timescaledb_internal._dist_hyper_15_68_chunk');
SELECT compress_chunk('_timescaledb_internal._dist_hyper_15_70_chunk');

-- Non-distributed chunk will be used to test an error
SELECT chunk_name 
FROM timescaledb_information.chunks 
WHERE hypertable_name = 'conditions';

\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.create_chunk_replica_table(NULL, 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', NULL);
SELECT _timescaledb_internal.create_chunk_replica_table(1234, 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('metrics_int', 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('conditions', 'data_node_1');
SELECT _timescaledb_internal. create_chunk_replica_table('_timescaledb_internal._hyper_10_51_chunk', 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_27_chunk', 'data_node_1');
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_4');
BEGIN READ ONLY;
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_3');
COMMIT;
\set ON_ERROR_STOP 1

\c data_node_3
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = '_timescaledb_internal' AND 
    (table_name LIKE '_dist_hyper_15_%' OR table_name LIKE 'compress_hyper_5_%');
\c :TEST_DBNAME 

SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_3');

-- Test that the table cannot be created since it was already created on the data node
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_67_chunk', 'data_node_3');
\set ON_ERROR_STOP 1

-- Creating chunk replica table ignores compression now:
SELECT _timescaledb_internal.create_chunk_replica_table('_timescaledb_internal._dist_hyper_15_70_chunk', 'data_node_3');

\c data_node_3
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = '_timescaledb_internal' AND 
    (table_name LIKE '_dist_hyper_15_%' OR table_name LIKE 'compress_hyper_5_%');
\c :TEST_DBNAME 

DROP TABLE dist_chunk_copy;
CALL distributed_exec($$ DROP TABLE _timescaledb_internal._dist_hyper_15_67_chunk $$, '{"data_node_3"}');
CALL distributed_exec($$ DROP TABLE _timescaledb_internal._dist_hyper_15_70_chunk $$, '{"data_node_3"}');
