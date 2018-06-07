-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- ===================================================================
-- create FDW objects
-- ===================================================================

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION remote_node_killer_set_event(text, text)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_remote_node_killer_set_event'
LANGUAGE C;

CREATE OR REPLACE FUNCTION remote_exec(srv_name name, sql_code text)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'ts_remote_exec'
LANGUAGE C;

CREATE OR REPLACE FUNCTION test_remote_txn_persistent_record(srv_name name)
RETURNS VOID
AS :TSL_MODULE_PATHNAME, 'tsl_test_remote_txn_persistent_record'
LANGUAGE C;

SELECT true FROM add_server('loopback', port=>current_setting('port')::integer);
SELECT true FROM add_server('loopback2', port=>current_setting('port')::integer);


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
BEGIN;
    SELECT remote_exec('loopback', $$ INSERT INTO "S 1"."T 1" VALUES (20001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 20001;

--aborted transaction
BEGIN;
    SELECT remote_exec('loopback', $$ INSERT INTO "S 1"."T 1" VALUES (20002,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
ROLLBACK;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 20002;

--constraint violation
BEGIN;
    SELECT remote_exec('loopback', $$ INSERT INTO "S 1"."T 1" VALUES (20001,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;

--the next few statements inject faults before the commit. They should all fail
--and be rolled back with no unresolved state
BEGIN;
	SELECT remote_node_killer_set_event('pre-commit', 'loopback');
    SELECT remote_exec('loopback', $$ INSERT INTO "S 1"."T 1" VALUES (20003,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;

SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 20003;
SELECT count(*) FROM pg_prepared_xacts;

BEGIN;
	SELECT remote_node_killer_set_event('waiting-commit', 'loopback');
    SELECT remote_exec('loopback', $$ INSERT INTO "S 1"."T 1" VALUES (20004,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
COMMIT;

--during waiting-commit the data node process could die before or after
--executing the commit on the remote node. So the behaviour here is non-deterministic
--this is the bad part of 1-pc transactions.
--there are no prepared txns in either case
SELECT count(*) FROM pg_prepared_xacts;

--fail the connection before the abort
BEGIN;
	SELECT remote_node_killer_set_event('pre-abort', 'loopback');
    SELECT remote_exec('loopback', $$ INSERT INTO "S 1"."T 1" VALUES (20005,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
ROLLBACK;
SELECT count(*) FROM "S 1"."T 1" WHERE "C 1" = 20005;
SELECT count(*) FROM pg_prepared_xacts;

--block preparing transactions on the frontend
BEGIN;
    SELECT remote_exec('loopback', $$ INSERT INTO "S 1"."T 1" VALUES (20006,1,'bleh', '2001-01-01', '2001-01-01', 'bleh') $$);
PREPARE TRANSACTION 'test-2';
