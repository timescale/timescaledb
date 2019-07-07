-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION create_records()
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'tsl_test_remote_txn_resolve_create_records'
LANGUAGE C;

CREATE OR REPLACE FUNCTION create_records_with_concurrent_heal()
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'tsl_test_remote_txn_resolve_create_records_with_concurrent_heal'
LANGUAGE C;

CREATE OR REPLACE FUNCTION add_loopback_server(
    server_name            NAME,
    host                   TEXT = 'localhost',
    database               NAME = current_database(),
    port                   INTEGER = inet_server_port(),
    password               TEXT = NULL,
    if_not_exists          BOOLEAN = FALSE,
    bootstrap_database     NAME = 'postgres',
    bootstrap_user         NAME = NULL,
    bootstrap_password     TEXT = NULL
) RETURNS TABLE(server_name NAME, host TEXT, port INTEGER, database NAME,
                server_created BOOL, database_created BOOL, extension_created BOOL)
AS :TSL_MODULE_PATHNAME, 'tsl_unchecked_add_data_node'
LANGUAGE C;

SELECT true FROM add_loopback_server('loopback', database => :'TEST_DBNAME', port=>current_setting('port')::integer, if_not_exists => true);
SELECT true FROM add_loopback_server('loopback2', database => :'TEST_DBNAME', port=>current_setting('port')::integer, if_not_exists => true);
SELECT true FROM add_loopback_server('loopback3', database => :'TEST_DBNAME', port=>current_setting('port')::integer, if_not_exists => true);

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
