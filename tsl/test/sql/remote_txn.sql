-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- ===================================================================
-- create FDW objects
-- ===================================================================

\c :TEST_DBNAME :ROLE_SUPERUSER

\ir include/remote_exec.sql

CREATE OR REPLACE FUNCTION remote_node_killer_set_event(text, text)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_remote_node_killer_set_event'
LANGUAGE C;

CREATE OR REPLACE FUNCTION test_remote_txn_persistent_record(srv_name name)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_test_remote_txn_persistent_record'
LANGUAGE C;

-- To ensure predictability, we want to kill the remote backend when it's in
-- the midst of processing the transaction. To ensure that the access node
-- sets the event handler and then takes an exclusive session lock on the
-- "remote_conn_xact_end" advisory lock via "debug_waitpoint_enable" function
--
-- The "RegisterXactCallback" callback on the remote backend tries to take
-- this same advisory lock in shared mode and waits. This allows the event
-- handler enough time to kill this remote backend at the right time
--
-- Don't forget to release lock via debug_waitpoint_release on the access node
-- since it's a session level advisory lock
--
CREATE OR REPLACE FUNCTION debug_waitpoint_enable(TEXT)
RETURNS VOID
AS :MODULE_PATHNAME, 'ts_debug_point_enable'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION debug_waitpoint_release(TEXT)
RETURNS VOID
AS :MODULE_PATHNAME, 'ts_debug_point_release'
LANGUAGE C VOLATILE STRICT;

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

SELECT server_name, database, server_created, database_created, extension_created FROM add_loopback_server('loopback', database => :'TEST_DBNAME', bootstrap => false);
SELECT server_name, database, server_created, database_created, extension_created FROM add_loopback_server('loopback2', database => :'TEST_DBNAME', bootstrap => false);

-- ===================================================================
-- create objects used through FDW loopback server
-- ===================================================================
CREATE SCHEMA "S 1";
CREATE TABLE "S 1"."T 1" (
    "C 1" int NOT NULL,
    c2 int NOT NULL,
    c3 text,
    c4 timestamptz,
    c5 timestamp,
    c6 varchar(10),
    c7 char(10),
    CONSTRAINT t1_pkey PRIMARY KEY ("C 1")
);

ANALYZE "S 1"."T 1";

INSERT INTO "S 1"."T 1"
    SELECT id,
           id % 10,
           to_char(id, 'FM00000'),
           '1970-01-01'::timestamptz + ((id % 100) || ' days')::interval,
           '1970-01-01'::timestamp + ((id % 100) || ' days')::interval,
           id % 10,
           id % 10
    FROM generate_series(1, 1000) id;

\set ON_ERROR_STOP 0

SELECT test_remote_txn_persistent_record('loopback');

-- ===================================================================
-- 1 pc tests
-- ===================================================================

--successfull transaction
SET timescaledb.enable_2pc = false;

--initially, there are no connections in the cache
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (20001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    --expect to see one connection in transaction state
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
COMMIT;

--connection should remain, idle
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 20001;

--aborted transaction
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (20002,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    --existing connection, in transaction
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
ROLLBACK;

--connection should remain, in idle state after rollback
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 20002;

--constraint violation
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (20001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;

-- Connection should remain after conflict, in idle state
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

--the next few statements inject faults before the commit. They should all fail
--and be rolled back with no unresolved state
BEGIN;
    SELECT remote_node_killer_set_event('pre-commit', 'loopback');
    SELECT debug_waitpoint_enable('remote_conn_xact_end');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (20003,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;
SELECT debug_waitpoint_release('remote_conn_xact_end');

-- Failed connection should be cleared
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 20003;
SELECT count(*) FROM pg_prepared_xacts;

BEGIN;
    SELECT remote_node_killer_set_event('waiting-commit', 'loopback');
    SELECT debug_waitpoint_enable('remote_conn_xact_end');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (20004,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    --connection in transaction
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
COMMIT;
SELECT debug_waitpoint_release('remote_conn_xact_end');

--connection failed during commit, so should be cleared from the cache
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

--during waiting-commit the data node process could die before or after
--executing the commit on the remote node. So the behaviour here is non-deterministic
--this is the bad part of 1-pc transactions.
--there are no prepared txns in either case
SELECT count(*) FROM pg_prepared_xacts;

--fail the connection before the abort
BEGIN;
    SELECT remote_node_killer_set_event('pre-abort', 'loopback');
    SELECT debug_waitpoint_enable('remote_conn_xact_end');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (20005,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    --connection in transaction
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

-- since the error messages/warnings from this ROLLBACK varies between
-- platforms/environments we remove it from test output and show
-- SQLSTATE instead. SQLSTATE should be 00000 since we are not
-- throwing errors during rollback/abort and it should succeed in any
-- case.
--<exclude_from_test>
ROLLBACK;
--</exclude_from_test>
\echo 'ROLLBACK SQLSTATE' :SQLSTATE
SELECT debug_waitpoint_release('remote_conn_xact_end');

SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 20005;
SELECT count(*) FROM pg_prepared_xacts;

--the failed connection should be cleared
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

--block preparing transactions on the access node
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (20006,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
PREPARE TRANSACTION 'test-2';


-- ===================================================================
-- 2 pc tests
-- ===================================================================
--undo changes from 1-pc tests
DELETE FROM  "S 1"."T 1" where "C 1" >= 20000;
SET timescaledb.enable_2pc = true;

SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

--simple commit
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    --connection in transaction
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
COMMIT;

--connection should remain in idle state
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10001;
SELECT count(*) FROM pg_prepared_xacts;

--simple
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (11001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    --connection in transaction
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
ROLLBACK;

--rolled back transaction, but connection should remain in idle
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 11001;
SELECT count(*) FROM pg_prepared_xacts;

--constraint violation should fail the txn
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;

--connection should remain in idle after constraint violation
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

--the next few statements inject faults before the commit. They should all fail
--and be rolled back with no unresolved state
BEGIN;
    SELECT remote_node_killer_set_event('pre-prepare-transaction', 'loopback');
    SELECT debug_waitpoint_enable('remote_conn_xact_end');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10002,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
-- since the error messages/warnings from this COMMIT varies between
-- platforms/environments we remove it from test output and show SQLSTATE instead.
-- SQLSTATE should be 08006 connection_failure
--<exclude_from_test>
COMMIT;
--</exclude_from_test>
\echo 'COMMIT SQLSTATE' :SQLSTATE
SELECT debug_waitpoint_release('remote_conn_xact_end');

--the connection was killed, so should be cleared
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10002;
SELECT count(*) FROM pg_prepared_xacts;

BEGIN;
    SELECT remote_node_killer_set_event('waiting-prepare-transaction', 'loopback');
    SELECT debug_waitpoint_enable('remote_conn_xact_end');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10003,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
-- since the error messages/warnings from this COMMIT varies between
-- platforms/environments we remove it from test output and show SQLSTATE instead.
-- SQLSTATE should be 08006 connection_failure
--<exclude_from_test>
COMMIT;
--</exclude_from_test>
\echo 'COMMIT SQLSTATE' :SQLSTATE
SELECT debug_waitpoint_release('remote_conn_xact_end');

--the connection should be cleared from the cache
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10003;

--during waiting-prepare-transaction the data node process could die before or after
--executing the prepare transaction. To be safe to either case rollback using heal_server.
SELECT true FROM _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));
SELECT count(*) FROM pg_prepared_xacts;
SELECT count(*) from _timescaledb_catalog.remote_txn;


--the following only breaks stuff in post-commit so commit should succeed
--but leave transaction in an unresolved state.
BEGIN;
    SELECT remote_node_killer_set_event('post-prepare-transaction', 'loopback');
    SELECT debug_waitpoint_enable('remote_conn_xact_end');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10004,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
-- since the error messages/warnings from this COMMIT varies between
-- platforms/environments we remove it from test output and show SQLSTATE instead.
-- SQLSTATE should be 00000 successful_completion
--<exclude_from_test>
COMMIT;
--</exclude_from_test>
\echo 'COMMIT SQLSTATE' :SQLSTATE
SELECT debug_waitpoint_release('remote_conn_xact_end');

--connection should be cleared
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

--unresolved state shown here
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10004;
SELECT count(*) FROM pg_prepared_xacts;

--this fails because heal cannot run inside txn block
BEGIN;
    SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));
COMMIT;

SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

select count(*) from _timescaledb_catalog.remote_txn;

--this resolves the previous txn
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10004;
SELECT count(*) FROM pg_prepared_xacts;
--cleanup also happened
select count(*) from _timescaledb_catalog.remote_txn;

BEGIN;
    SELECT remote_node_killer_set_event('pre-commit-prepared', 'loopback');
    SELECT debug_waitpoint_enable('remote_conn_xact_end');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10006,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
-- since the error messages/warnings from this COMMIT varies between
-- platforms/environments we remove it from test output and show SQLSTATE instead.
-- SQLSTATE should be 00000 successful_completion
--<exclude_from_test>
COMMIT;
--</exclude_from_test>
\echo 'COMMIT SQLSTATE' :SQLSTATE
SELECT debug_waitpoint_release('remote_conn_xact_end');

SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

--unresolved state shown here
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10006;
SELECT count(*) FROM pg_prepared_xacts;
--this resolves the previous txn
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10006;
SELECT count(*) FROM pg_prepared_xacts;
select count(*) from _timescaledb_catalog.remote_txn;

SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

BEGIN;
    SELECT remote_node_killer_set_event('waiting-commit-prepared','loopback');
    SELECT debug_waitpoint_enable('remote_conn_xact_end');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10005,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
-- since the error messages/warnings from this COMMIT varies between
-- platforms/environments we remove it from test output and show SQLSTATE instead.
-- SQLSTATE should be 00000 successful_completion
--<exclude_from_test>
COMMIT;
--</exclude_from_test>
\echo 'COMMIT SQLSTATE' :SQLSTATE
SELECT debug_waitpoint_release('remote_conn_xact_end');

--at this point the commit prepared might or might not have been executed before
--the data node process was killed.
--but in any case, healing the server will bring it into a known state
SELECT true FROM _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));

--heal does not use the connection cache, so unaffected
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10005;
SELECT count(*) FROM pg_prepared_xacts;
select count(*) from _timescaledb_catalog.remote_txn;

--test prepare transactions. Note that leaked prepared stmts should be
--detected by `remote_txn_check_for_leaked_prepared_statements` so we
--should be fine if we don't see any WARNINGS.
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ PREPARE prep_1 AS SELECT 1 $$);
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;
--unique constraint violation, connection should remain
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

BEGIN;
    SAVEPOINT save_1;
        SELECT test.remote_exec('{loopback}', $$ PREPARE prep_1 AS SELECT 1 $$);

        --connection in transaction state
        SELECT node_name, connection_status, transaction_status, transaction_depth, processing
        FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
        --generate a unique violation
        SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);

    ROLLBACK TO SAVEPOINT save_1;

    --for correctness, the connection must remain after rollback since
    --the transaction is still ongoing
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (81,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;

--connection should remain and be idle
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 81;


--Make the primary key DEFERRABLE and INITIALLY DEFERRED
--this is a way to force errors to happen during PREPARE TRANSACTION
--since pkey constraint violations would not occur on the INSERT command
--but rather are deferred till PREPARE TRANSACTION happens.
ALTER TABLE "S 1"."T 1" DROP CONSTRAINT t1_pkey,
ADD CONSTRAINT t1_pkey PRIMARY KEY ("C 1") DEFERRABLE INITIALLY DEFERRED;

--test ROLLBACK TRANSACTION on failure in PREPARE TRANSACTION.
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
COMMIT;

--connection should be removed since PREPARE TRANSACTION failed
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
SELECT count(*) FROM pg_prepared_xacts;

--test ROLLBACK TRANSACTION
--this has an error on the second connection. So should force conn1 to prepare transaction
--ok and then have the txn fail on conn2. Thus conn1 would do a ROLLBACK PREPARED.
--conn2 would do a ROLLBACK TRANSACTION.
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10010,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    SELECT test.remote_exec('{loopback2}', $$ INSERT INTO "S 1"."T 1" VALUES (10001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);

    --Both connections in transaction state
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
COMMIT;

--one connection should remain and be idle
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10010;
SELECT count(*) FROM pg_prepared_xacts;

-- Below will fail the abort and thus ROLLBACK TRANSACTION will never
-- be called leaving a prepared_xact that should be rolled back by
-- heal server.
--
-- We set min message level to "error" since different warnings can be
-- generated due to timing issues but check that the transaction was
-- rolled back after the commit.
SET client_min_messages TO error;
BEGIN;
    SELECT remote_node_killer_set_event('pre-abort', 'loopback');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10011,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    SELECT test.remote_exec('{loopback2}', $$ INSERT INTO "S 1"."T 1" VALUES (10001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);

    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
COMMIT;
RESET client_min_messages;

--failed connection should be cleared
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10011;
SELECT count(*) FROM pg_prepared_xacts;
SELECT _timescaledb_internal.remote_txn_heal_data_node((SELECT OID FROM pg_foreign_server WHERE srvname = 'loopback'));
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 10011;
SELECT count(*) FROM pg_prepared_xacts;

--heal is not using the connection cache
SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;

--test simple subtrans abort.
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10012,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    SELECT test.remote_exec('{loopback2}', $$ INSERT INTO "S 1"."T 1" VALUES (10013,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);

    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
    SAVEPOINT save_1;
        SELECT test.remote_exec('{loopback2}', $$ INSERT INTO "S 1"."T 1" VALUES (10001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
        SELECT node_name, connection_status, transaction_status, transaction_depth, processing
        FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
    ROLLBACK TO SAVEPOINT save_1;

    -- For correctness, both connections should remain in order to
    -- continue the transaction
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10014,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    SELECT test.remote_exec('{loopback2}', $$ INSERT INTO "S 1"."T 1" VALUES (10015,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);

    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
COMMIT;

SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" > 10011;
SELECT count(*) FROM pg_prepared_xacts;

-- Test comm error in subtrans abort
--
-- We set min message level to "error" since different warnings can be
-- generated due to timing issues but check that the transaction was
-- rolled back after the commit.
SET client_min_messages TO error;
BEGIN;
    SELECT remote_node_killer_set_event('subxact-abort', 'loopback');
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10017,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    SELECT test.remote_exec('{loopback2}', $$ INSERT INTO "S 1"."T 1" VALUES (10018,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    SAVEPOINT save_1;
        SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
    ROLLBACK TO SAVEPOINT save_1;
    SELECT node_name, connection_status, transaction_status, transaction_depth, processing
    FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10019,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
    SELECT test.remote_exec('{loopback2}', $$ INSERT INTO "S 1"."T 1" VALUES (10020,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;
RESET client_min_messages;

SELECT node_name, connection_status, transaction_status, transaction_depth, processing
FROM _timescaledb_internal.show_connection_cache() ORDER BY 1,4;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" > 10016;
SELECT count(*) FROM pg_prepared_xacts;


--block preparing transactions on the frontend
BEGIN;
    SELECT test.remote_exec('{loopback}', $$ INSERT INTO "S 1"."T 1" VALUES (10051,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
PREPARE TRANSACTION 'test-1';

-- test remote_txn cleanup on data node delete
--
SELECT count(*) FROM _timescaledb_catalog.remote_txn WHERE data_node_name = 'loopback' or data_node_name = 'loopback2';

SELECT * FROM delete_data_node('loopback');
SELECT count(*) FROM _timescaledb_catalog.remote_txn WHERE data_node_name = 'loopback';

SELECT * FROM delete_data_node('loopback2');
SELECT count(*) FROM _timescaledb_catalog.remote_txn WHERE data_node_name = 'loopback2';
