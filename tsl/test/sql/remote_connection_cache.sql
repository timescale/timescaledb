-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

\set DN_DBNAME_1 :TEST_DBNAME _1
\set DN_DBNAME_2 :TEST_DBNAME _2

CREATE FUNCTION _timescaledb_internal.test_remote_connection_cache()
RETURNS void
AS :TSL_MODULE_PATHNAME, 'ts_test_remote_connection_cache'
LANGUAGE C STRICT;

CREATE FUNCTION _timescaledb_internal.test_alter_data_node(node_name NAME)
RETURNS BOOL
AS :TSL_MODULE_PATHNAME, 'ts_test_alter_data_node'
LANGUAGE C STRICT;

SET client_min_messages TO WARNING;
SELECT node_name, database, node_created, extension_created
FROM add_data_node('loopback_1', host => 'localhost', database => :'DN_DBNAME_1',
                   port => current_setting('port')::int);
SELECT node_name, database, node_created, extension_created
FROM add_data_node('loopback_2', host => 'localhost', database => :'DN_DBNAME_2',
                   port => current_setting('port')::int);
SET client_min_messages TO INFO;

SELECT _timescaledb_internal.test_remote_connection_cache();

-- Test that connection cache entries for a role gets invalidated when
-- we rename the role
GRANT USAGE ON FOREIGN SERVER loopback_1, loopback_2 TO :ROLE_1;
SET ROLE :ROLE_1;

CREATE TABLE testtable (time timestamptz, location int, temp float);
SELECT * FROM create_distributed_hypertable('testtable', 'time', 'location');
INSERT INTO testtable VALUES ('2021-09-19', 1, 13.2);

-- Should show valid connections for ROLE_1
SELECT node_name, user_name, invalidated
FROM _timescaledb_internal.show_connection_cache()
WHERE user_name=:'ROLE_1'
ORDER BY 1,2;
RESET ROLE;
BEGIN;

-- Renaming the role should invalidate the connection cache entries
-- for ROLE_1/bob. The connections will be recreated on next cache
-- fetch.
ALTER ROLE :ROLE_1 RENAME TO bob;
SELECT node_name, user_name, invalidated
FROM _timescaledb_internal.show_connection_cache()
WHERE user_name='bob'
ORDER BY 1,2;
ROLLBACK;

DROP DATABASE :DN_DBNAME_1;
DROP DATABASE :DN_DBNAME_2;
