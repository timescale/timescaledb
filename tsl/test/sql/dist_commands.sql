-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

SELECT * FROM add_server('server1', 'localhost', 'server1', port=>current_setting('port')::integer);
SELECT * FROM add_server('server2', 'localhost', 'server2', port=>current_setting('port')::integer);
SELECT * FROM add_server('server3', 'localhost', 'server3', port=>current_setting('port')::integer);

\des+

CREATE FUNCTION _timescaledb_internal.invoke_distributed_commands()
RETURNS void
AS :TSL_MODULE_PATHNAME, 'tsl_invoke_distributed_commands'
LANGUAGE C STRICT;

CREATE FUNCTION _timescaledb_internal.invoke_faulty_distributed_command()
RETURNS void
AS :TSL_MODULE_PATHNAME, 'tsl_invoke_faulty_distributed_command'
LANGUAGE C STRICT;

SELECT _timescaledb_internal.invoke_distributed_commands();

\c server1
\dt
SELECT * FROM disttable1;
\c server2
\dt
SELECT * FROM disttable1;
\c server3
\dt
SELECT * FROM disttable1;
\c single

-- Verify failed insert command gets fully rolled back
\set ON_ERROR_STOP 0
SELECT _timescaledb_internal.invoke_faulty_distributed_command();
\set ON_ERROR_STOP 1

\c server1
SELECT * from disttable2;
\c server3
SELECT * from disttable2;

\c single
DROP DATABASE server1;
DROP DATABASE server2;
DROP DATABASE server3;
