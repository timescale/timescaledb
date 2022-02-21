-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\o
\set ECHO all

\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2
\set DN_DBNAME_3 :TEST_DBNAME _3

SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => :'DN_DBNAME_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => :'DN_DBNAME_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost',
                            database => :'DN_DBNAME_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;

SET ROLE :ROLE_1;

CREATE TABLE dist_test(time timestamp NOT NULL, device int, temp float);
SELECT create_distributed_hypertable('dist_test', 'time', 'device', 3);
INSERT INTO dist_test SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;
SELECT * from show_chunks('dist_test');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('dist_test'); $$);

SELECT sum(device) FROM dist_test; 
SELECT * FROM test.remote_exec(ARRAY['data_node_1'], $$ SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk; $$);

-- ensure data node name is provided and has proper type
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> null, destination_node => 'data_node_2');
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => null);
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 2);
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node => 'data_node_1');
\set ON_ERROR_STOP 1

-- ensure functions can't be run in read only mode
SET default_transaction_read_only TO on;
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
\set ON_ERROR_STOP 1
SET default_transaction_read_only TO off;

-- ensure functions can't be run in an active multi-statement transaction
\set ON_ERROR_STOP 0
BEGIN;
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
ROLLBACK;
BEGIN;
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
ROLLBACK;
\set ON_ERROR_STOP 1

-- must be superuser to copy/move chunks
SET ROLE :ROLE_DEFAULT_PERM_USER;
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
\set ON_ERROR_STOP 1
SET ROLE :ROLE_1;

-- can't run copy/move chunk on a data node
\c :DN_DBNAME_1 :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- ensure that hypertable chunks are distributed
CREATE TABLE nondist_test(time timestamp NOT NULL, device int, temp float);
SELECT create_hypertable('nondist_test', 'time', 'device', 3);
INSERT INTO nondist_test SELECT t, (abs(timestamp_hash(t::timestamp)) % 10) + 1, 0.10 FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-08 1:00', '1 hour') t;
SELECT * from show_chunks('nondist_test');
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._hyper_2_5_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._hyper_2_5_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
\set ON_ERROR_STOP 1

-- ensure that distributed chunk is not compressed
ALTER TABLE dist_test SET (timescaledb.compress, timescaledb.compress_segmentby='device', timescaledb.compress_orderby = 'time DESC');
SELECT compress_chunk('_timescaledb_internal._dist_hyper_1_4_chunk');
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_4_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_4_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
\set ON_ERROR_STOP 1

-- ensure that chunk exists on a source data node
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_2_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
\set ON_ERROR_STOP 1

-- do actualy copy
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('dist_test'); $$);
SELECT * FROM test.remote_exec(ARRAY['data_node_2'], $$ SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk; $$);

-- ensure that chunk exists on a destination data node
\set ON_ERROR_STOP 0
CALL timescaledb_experimental.copy_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_1', destination_node => 'data_node_2');
\set ON_ERROR_STOP 1

-- now try to move the same chunk from data node 2 to 3
CALL timescaledb_experimental.move_chunk(chunk=>'_timescaledb_internal._dist_hyper_1_1_chunk', source_node=> 'data_node_2', destination_node => 'data_node_3');
SELECT * FROM test.remote_exec(NULL, $$ SELECT * from show_chunks('dist_test'); $$);
SELECT * FROM test.remote_exec(ARRAY['data_node_3'], $$ SELECT sum(device) FROM _timescaledb_internal._dist_hyper_1_1_chunk; $$);
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

CALL copy_wrapper('_timescaledb_internal._dist_hyper_1_3_chunk', 'data_node_3', 'data_node_2');
CALL move_wrapper('_timescaledb_internal._dist_hyper_1_2_chunk', 'data_node_2', 'data_node_1');

SELECT chunk_name, replica_nodes, non_replica_nodes
FROM timescaledb_experimental.chunk_replication_status;

DROP PROCEDURE copy_wrapper;
DROP PROCEDURE move_wrapper;

RESET ROLE;
DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
DROP DATABASE :DN_DBNAME_3;
