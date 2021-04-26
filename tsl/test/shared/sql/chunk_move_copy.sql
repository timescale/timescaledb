-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE mvcp_hyper (time bigint NOT NULL, value integer);
SELECT table_name FROM create_distributed_hypertable('mvcp_hyper', 'time',
        chunk_time_interval => 200, replication_factor => 3);

-- Enable compression so that we can test dropping of compressed chunks
ALTER TABLE mvcp_hyper  SET (timescaledb.compress, timescaledb.compress_orderby='time DESC');

INSERT INTO mvcp_hyper SELECT g, g FROM generate_series(0,1000) g;

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
SELECT ch1.schema_name|| '.' || ch1.table_name as "CHUNK_NAME", ch1.id "CHUNK_ID" FROM
_timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht WHERE ch1.hypertable_id = ht.id
AND ht.table_name = 'mvcp_hyper' ORDER BY ch1.id desc LIMIT 1 \gset

-- Specifying wrong node name errors out
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', 'bad_node');

-- This chunk contains only one entry as of now
SELECT * FROM :CHUNK_NAME;

-- Specifying NULL node name along with proper chunk errors out
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', NULL);

\set ON_ERROR_STOP 1
-- Check the current primary foreign server for this chunk, that will change
-- post the chunk_drop_replica call
SELECT foreign_server_name FROM information_schema.foreign_tables WHERE
    foreign_table_name = split_part(:'CHUNK_NAME', '.', 2);

-- Drop one replica of a valid chunk. Should succeed
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', 'data_node_3');

-- The primary foreign server should be updated now
SELECT foreign_server_name FROM information_schema.foreign_tables WHERE
    foreign_table_name = split_part(:'CHUNK_NAME', '.', 2);

-- Number of replicas should have been reduced by 1
SELECT count(*) FROM _timescaledb_catalog.chunk_data_node WHERE chunk_id = :'CHUNK_ID';

-- Ensure that INSERTs still work on this mvcp_hyper table into this chunk
INSERT INTO mvcp_hyper VALUES (1001, 1001);
-- Ensure that SELECTs are able to query data from the above chunk
SELECT count(*) FROM mvcp_hyper WHERE time >= 1000;

-- Check that chunk_drop_replica works with compressed chunk
SELECT substr(compress_chunk(:'CHUNK_NAME')::TEXT, 1, 29);

-- Drop one replica of a valid chunk. Should succeed on another datanode
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', 'data_node_2');

-- Number of replicas should have been reduced by 1
SELECT count(*) FROM _timescaledb_catalog.chunk_data_node WHERE chunk_id = :'CHUNK_ID';

-- Decompress before checking INSERTs
SELECT substr(decompress_chunk(:'CHUNK_NAME')::TEXT, 1, 29);

-- Ensure that INSERTs still work on this mvcp_hyper table into this chunk
INSERT INTO mvcp_hyper VALUES (1002, 1002);
-- Ensure that SELECTs are able to query data from the above chunk
SELECT count(*) FROM mvcp_hyper WHERE time >= 1000;

\set ON_ERROR_STOP 0
-- Drop one replica of a valid chunk. Should not succeed on last datanode
SELECT _timescaledb_internal.chunk_drop_replica(:'CHUNK_NAME', 'data_node_1');
\set ON_ERROR_STOP 1

DROP table mvcp_hyper;
