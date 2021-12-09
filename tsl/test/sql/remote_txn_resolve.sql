-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION create_records()
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_remote_txn_resolve_create_records'
LANGUAGE C;

CREATE OR REPLACE FUNCTION create_prepared_record()
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_remote_txn_resolve_create_prepared_record'
LANGUAGE C;

CREATE OR REPLACE FUNCTION create_records_with_concurrent_heal()
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_remote_txn_resolve_create_records_with_concurrent_heal'
LANGUAGE C;

CREATE OR REPLACE FUNCTION add_loopback_server(
    server_name            NAME,
    host                   TEXT = 'localhost',
    database               NAME = current_database(),
    port                   INTEGER = inet_server_port(),
    if_not_exists          BOOLEAN = FALSE,
    bootstrap              BOOLEAN = TRUE,
    password               TEXT = NULL
) RETURNS TABLE(server_name NAME, host TEXT, port INTEGER, database NAME,
                server_created BOOL, database_created BOOL, extension_created BOOL)
AS :TSL_MODULE_PATHNAME, 'ts_unchecked_add_data_node'
LANGUAGE C;

SELECT true FROM add_loopback_server('loopback', database => :'TEST_DBNAME', bootstrap => false);
SELECT true FROM add_loopback_server('loopback2', database => :'TEST_DBNAME', bootstrap => false);
SELECT true FROM add_loopback_server('loopback3', database => :'TEST_DBNAME', bootstrap => false);

create table table_modified_by_txns (
    describes text
);

--create records will create 3 records
--1) that is committed
--2) that is prepared but not committed
--3) that is prepared and rolled back
--Thus (1) will be seen right away, (2) will be seen after the heal, (3) will never be seen
SELECT create_records();

SELECT * FROM table_modified_by_txns;
SELECT count(*) FROM pg_prepared_xacts;
SELECT count(*) FROM _timescaledb_catalog.remote_txn;

SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback2'));
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback3'));

SELECT * FROM table_modified_by_txns;
SELECT count(*) FROM pg_prepared_xacts;
SELECT count(*) FROM _timescaledb_catalog.remote_txn;

--insert one record where the heal function is run concurrently during different steps of the process
--this tests safety when, for example, the heal function is run while the frontend txn is still ongoing.
SELECT create_records_with_concurrent_heal();
SELECT * FROM table_modified_by_txns;
SELECT count(*) FROM pg_prepared_xacts;
SELECT count(*) FROM _timescaledb_catalog.remote_txn;

SELECT create_records();
-- create an additional prepared entry. This will allow us to test heal behavior when one
-- attempt errors out and when the other should succeed. The debug_inject_gid_error logic
-- only induces one error for now. This can be modified later as desired via another
-- session variable.
SELECT create_prepared_record();
--inject errors in the GID and test "commit" resolution for it
SET timescaledb.debug_inject_gid_error TO 'commit';
--heal should error out and the prepared transaction should still be visible
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback2'));
SELECT * FROM table_modified_by_txns;
SELECT count(*) FROM pg_prepared_xacts;
SELECT count(*) FROM _timescaledb_catalog.remote_txn;

-- Again process 2 records where one errors out and other succeeds
SELECT create_prepared_record();
--inject errors in the GID and test "abort" resolution for it
SET timescaledb.debug_inject_gid_error TO 'abort';
--heal should error out and the prepared transaction should still be visible
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback2'));
SELECT * FROM table_modified_by_txns;
SELECT count(*) FROM pg_prepared_xacts;
SELECT count(*) FROM _timescaledb_catalog.remote_txn;

-- Again process 2 records where one errors out and other succeeds
SELECT create_prepared_record();
--test "inprogress" resolution for the prepared 2PC
SET timescaledb.debug_inject_gid_error TO 'inprogress';
--heal will not error out but the prepared transaction should still be visible
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback2'));
SELECT * FROM table_modified_by_txns;
SELECT count(*) FROM pg_prepared_xacts;
SELECT count(*) FROM _timescaledb_catalog.remote_txn;

-- Again process 2 records where one errors out and other succeeds
SELECT create_prepared_record();
--set to any random value so that it does not have any effect and allows healing
SET timescaledb.debug_inject_gid_error TO 'ignored';
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback2'));
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback3'));
SELECT * FROM table_modified_by_txns;
SELECT count(*) FROM pg_prepared_xacts;
SELECT count(*) FROM _timescaledb_catalog.remote_txn;

--test that it is safe to have non-ts prepared-txns with heal
BEGIN;
    INSERT INTO public.table_modified_by_txns VALUES ('non-ts-txn');
PREPARE TRANSACTION 'non-ts-txn';

SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback2'));
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback3'));

COMMIT PREPARED 'non-ts-txn';
SELECT * FROM table_modified_by_txns;
SELECT count(*) FROM pg_prepared_xacts;
SELECT count(*) FROM _timescaledb_catalog.remote_txn;

-- test that healing function does not conflict with other databases
--
-- #3433

-- create additional database and simulate remote txn activity
CREATE DATABASE test_an2;
\c test_an2
BEGIN;
CREATE TABLE unused(id int);
PREPARE TRANSACTION 'ts-1-10-20-30';
\c :TEST_DBNAME :ROLE_SUPERUSER
-- should not fail
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));
\c test_an2
ROLLBACK PREPARED 'ts-1-10-20-30';
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE test_an2;
