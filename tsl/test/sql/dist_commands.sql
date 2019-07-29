-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

SELECT * FROM add_data_node('data_node1',
                            database => 'data_node1',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER');
SELECT * FROM add_data_node('data_node2',
                            database => 'data_node2',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER');
SELECT * FROM add_data_node('data_node3',
                            database => 'data_node3',
                            bootstrap_user => :'ROLE_CLUSTER_SUPERUSER');

\des+

RESET ROLE;
CREATE FUNCTION _timescaledb_internal.invoke_distributed_commands()
RETURNS void
AS :TSL_MODULE_PATHNAME, 'tsl_invoke_distributed_commands'
LANGUAGE C STRICT;

CREATE FUNCTION _timescaledb_internal.invoke_faulty_distributed_command()
RETURNS void
AS :TSL_MODULE_PATHNAME, 'tsl_invoke_faulty_distributed_command'
LANGUAGE C STRICT;
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

SELECT _timescaledb_internal.invoke_distributed_commands();

\c data_node1
\dt
SELECT * FROM disttable1;
\c data_node2
\dt
SELECT * FROM disttable1;
\c data_node3
\dt
SELECT * FROM disttable1;
\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;

-- Verify failed insert command gets fully rolled back
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.invoke_faulty_distributed_command();
\set ON_ERROR_STOP 1

\c data_node1
SELECT * from disttable2;
\c data_node2
SELECT * from disttable2;

-- Test connection session identity
\c :TEST_DBNAME :ROLE_SUPERUSER
\unset ECHO
\o /dev/null
\ir include/remote_exec.sql
\o
\set ECHO all

-- Register is_frontend_session() function and test that it returns false for
-- connections openned by test suite. This simualates behaviour expected
-- with a client connections.
CREATE OR REPLACE FUNCTION is_frontend_session()
RETURNS BOOL
AS :TSL_MODULE_PATHNAME, 'test_is_frontend_session' LANGUAGE C;

\c data_node1
CREATE OR REPLACE FUNCTION is_frontend_session()
RETURNS BOOL
AS :TSL_MODULE_PATHNAME, 'test_is_frontend_session' LANGUAGE C;
SELECT is_frontend_session();

\c data_node2
CREATE OR REPLACE FUNCTION is_frontend_session()
RETURNS BOOL
AS :TSL_MODULE_PATHNAME, 'test_is_frontend_session' LANGUAGE C;
SELECT is_frontend_session();

\c data_node3
CREATE OR REPLACE FUNCTION is_frontend_session()
RETURNS BOOL
AS :TSL_MODULE_PATHNAME, 'test_is_frontend_session' LANGUAGE C;
SELECT is_frontend_session();

\c :TEST_DBNAME :ROLE_SUPERUSER
SET ROLE :ROLE_DEFAULT_CLUSTER_USER;
SELECT is_frontend_session();

-- Ensure peer dist id is already set and can be set only once
\set ON_ERROR_STOP 0
SELECT * FROM test.remote_exec('{data_node1}', $$ SELECT * FROM _timescaledb_internal.set_peer_dist_id('77348176-09da-4a80-bc78-e31bdf5e63ec'); $$);
\set ON_ERROR_STOP 1

-- Repeat is_frontend_session() test again, but this time using connections openned from frontend
-- to backend nodes. Must return true.
SELECT * FROM test.remote_exec(NULL, $$ SELECT is_frontend_session(); $$);

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE data_node1;
DROP DATABASE data_node2;
DROP DATABASE data_node3;
