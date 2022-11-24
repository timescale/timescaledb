-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Need to be super user to create extension and add data nodes
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\ir include/filter_exec.sql
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

-- Import testsupport.sql file to data nodes
\unset ECHO
\o /dev/null
\c :DATA_NODE_1
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :DATA_NODE_2
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :DATA_NODE_3
SET client_min_messages TO ERROR;
\ir :TEST_SUPPORT_FILE
\c :TEST_DBNAME :ROLE_SUPERUSER;
\o
SET client_min_messages TO NOTICE;
\set ECHO all

-- test NULL (STRICT function)
SELECT create_distributed_restore_point(NULL);

-- test long restore point name (>= 64 chars)
\set ON_ERROR_STOP 0
SELECT create_distributed_restore_point('0123456789012345678901234567890123456789012345678901234567890123456789');
\set ON_ERROR_STOP 1

-- test super user check
\set ON_ERROR_STOP 0
SET ROLE :ROLE_1;
SELECT create_distributed_restore_point('test');
RESET ROLE;
\set ON_ERROR_STOP 1

-- make sure 2pc are enabled
SET timescaledb.enable_2pc = false;
\set ON_ERROR_STOP 0
SELECT create_distributed_restore_point('test');
\set ON_ERROR_STOP 1

SET timescaledb.enable_2pc = true;
SHOW timescaledb.enable_2pc;

-- make sure wal_level is replica or logical
SHOW wal_level;

-- make sure not in recovery mode
SELECT pg_is_in_recovery();

-- test on single node (not part of a cluster)
CREATE DATABASE dist_rp_test;
\c dist_rp_test :ROLE_CLUSTER_SUPERUSER
SET client_min_messages TO ERROR;
CREATE EXTENSION timescaledb;
\set ON_ERROR_STOP 0
SELECT create_distributed_restore_point('test');
\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;
DROP DATABASE dist_rp_test;

-- test on data node
\c :DATA_NODE_1 :ROLE_CLUSTER_SUPERUSER;
\set ON_ERROR_STOP 0
SELECT create_distributed_restore_point('test');
\set ON_ERROR_STOP 1
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER;

-- test with distributed_exec()
\set ON_ERROR_STOP 0
SELECT test.execute_sql_and_filter_data_node_name_on_error($$
CALL distributed_exec('SELECT create_distributed_restore_point(''test'')')
$$);
\set ON_ERROR_STOP 1

-- test on access node
SELECT node_name, node_type, pg_lsn(restore_point) > pg_lsn('0/0') as valid_lsn FROM create_distributed_restore_point('dist_rp') ORDER BY node_name;

-- restore point can be have duplicates
SELECT node_name, node_type, pg_lsn(restore_point) > pg_lsn('0/0') as valid_lsn FROM create_distributed_restore_point('dist_rp') ORDER BY node_name;

-- make sure each new restore point have lsn greater then previous one (access node lsn)
SELECT restore_point as lsn_1 FROM create_distributed_restore_point('dist_rp_1') WHERE node_type = 'access_node' \gset
SELECT restore_point as lsn_2 FROM create_distributed_restore_point('dist_rp_2') WHERE node_type = 'access_node' \gset
SELECT pg_lsn(:'lsn_2') > pg_lsn(:'lsn_1') as valid_lsn;

-- make sure it is compatible with local restore point
SELECT pg_create_restore_point('dist_rp') as lsn_3 \gset
SELECT pg_lsn(:'lsn_3') > pg_lsn(:'lsn_2') as valid_lsn;

-- test create_distributed_restore_point() when one of the nodes if unavailable
SELECT alter_data_node(:'DATA_NODE_1', available => false);

\set ON_ERROR_STOP 0
SELECT create_distributed_restore_point('test');
\set ON_ERROR_STOP 1

SELECT alter_data_node(:'DATA_NODE_1', available => true);
SELECT node_name, node_type, pg_lsn(restore_point) > pg_lsn('0/0') as valid_lsn FROM create_distributed_restore_point('test') ORDER BY node_name;

DROP DATABASE :DATA_NODE_1;
DROP DATABASE :DATA_NODE_2;
DROP DATABASE :DATA_NODE_3;
