-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\unset ECHO
\o /dev/null
\ir include/filter_exec.sql
\ir include/remote_exec.sql
\o
\set ECHO all

-- Cleanup from other potential tests that created these databases
SET client_min_messages TO ERROR;
DROP DATABASE IF EXISTS data_node_1;
DROP DATABASE IF EXISTS data_node_2;
DROP DATABASE IF EXISTS data_node_3;
SET client_min_messages TO NOTICE;

-- Add data nodes using the TimescaleDB node management API
SELECT * FROM add_data_node('data_node_1', host => 'localhost',
                            database => 'data_node_1');
SELECT * FROM add_data_node('data_node_2', host => 'localhost',
                            database => 'data_node_2');
SELECT * FROM add_data_node('data_node_3', host => 'localhost',
                            database => 'data_node_3');
GRANT USAGE ON FOREIGN SERVER data_node_1, data_node_2, data_node_3 TO PUBLIC;

SET ROLE :ROLE_1;

-- System columns are not supported with data node queries
SET timescaledb.enable_per_data_node_queries = FALSE;

-- Create distributed hypertables.
CREATE TABLE disttable_with_relopts_1(time timestamptz NOT NULL, device int CHECK (device > 0));
SELECT * FROM create_distributed_hypertable('disttable_with_relopts_1', 'time', 'device', 2);

-- Create some chunks
INSERT INTO disttable_with_relopts_1 VALUES
       ('2017-01-01 06:01', 1),
       ('2017-01-01 09:11', 3),
       ('2017-01-01 08:01', 1),
       ('2017-01-02 08:01', 2),
       ('2018-07-02 08:01', 87);

-- WITH OIDS
ALTER TABLE disttable_with_relopts_1 SET WITH OIDS;

SELECT relname, relhasoids FROM pg_class WHERE relname = 'disttable_with_relopts_1' ORDER BY relname;

-- Ensure chunks are not affected on the AN
SELECT relname, relhasoids FROM pg_class WHERE relname IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name FROM show_chunks('disttable_with_relopts_1'))
ORDER BY relname;

-- Ensure data node chunks properly updated
SELECT * FROM test.remote_exec('{ data_node_1, data_node_2, data_node_3 }',$$
    SELECT relname, relhasoids FROM pg_class WHERE relname IN
    (SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name FROM show_chunks('disttable_with_relopts_1'))
    ORDER BY relname;
$$);

SELECT true FROM disttable_with_relopts_1 WHERE oid >= 0 LIMIT 1;

-- Test WITHOUT OIDS -- NOTE that this is still supported in PG12 for
-- backwards compatibilty, but does nothing since WITH OIDS is not
-- supported
ALTER TABLE disttable_with_relopts_1 SET WITHOUT OIDS;

SELECT relname, relhasoids FROM pg_class WHERE relname = 'disttable_with_relopts_1' ORDER BY relname;

-- Ensure chunks are not affected on the AN
SELECT relname, relhasoids FROM pg_class WHERE relname IN
(SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name FROM show_chunks('disttable_with_relopts_1'))
ORDER BY relname;

-- Ensure data node chunks properly updated
SELECT * FROM test.remote_exec('{ data_node_1, data_node_2, data_node_3 }',$$
    SELECT relname, relhasoids FROM pg_class WHERE relname IN
    (SELECT (_timescaledb_internal.show_chunk(show_chunks)).table_name FROM show_chunks('disttable_with_relopts_1'))
    ORDER BY relname;
$$);

\set ON_ERROR_STOP 0
-- OID column removed
SELECT true FROM disttable_with_relopts_1 WHERE oid >= 0 LIMIT 1;
\set ON_ERROR_STOP 1

DROP TABLE disttable_with_relopts_1;
