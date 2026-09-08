-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION test.setup_tss_hook_v0() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_setup_tss_hook_v0' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.setup_tss_hook_v1() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_setup_tss_hook_v1' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.setup_tss_hook_version(BIGINT) RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_setup_tss_hook_version' LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION test.teardown_tss_hook_v1() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_teardown_tss_hook_v1' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.setup_tss_scope_hook_v1() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_setup_tss_scope_hook_v1' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.teardown_tss_scope_hook_v1() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_teardown_tss_scope_hook_v1' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.setup_tss_scope_hook_invalid(INTEGER) RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_setup_tss_scope_hook_invalid' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.arm_tss_scope_disable_guc() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_arm_tss_scope_disable_guc' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.arm_tss_scope_reject_entry() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_arm_tss_scope_reject_entry' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.set_tss_tracking_enabled(BOOLEAN) RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_set_tss_tracking_enabled' LANGUAGE C VOLATILE STRICT;

SELECT test.setup_tss_hook_v1();
-- The mock returns tokens above 32 bits to verify enter/exit round trips.
SELECT test.setup_tss_scope_hook_v1();

CREATE TABLE copy_test (
    "time" timestamptz NOT NULL,
    "value" double precision NOT NULL
);

SELECT create_hypertable('copy_test', 'time');

-- A current provider scopes COPY's internal statements and stores the synthetic COPY.
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- Errors unwind the scope before the next statement starts.
\set ON_ERROR_STOP 0
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,not-a-double
\.
\set ON_ERROR_STOP 1

-- The begin decision is stable even if enable_tss_callbacks changes in the scope.
SELECT test.arm_tss_scope_disable_guc();
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- A statement beginning while callbacks are disabled invokes neither ABI.
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

SET timescaledb.enable_tss_callbacks = on;

-- A declined scope still stores COPY, without calling scope exit.
SELECT test.arm_tss_scope_reject_entry();
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- A provider declining tracking invokes neither scope entry nor storage.
SELECT test.set_tss_tracking_enabled(false);
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.
SELECT test.set_tss_tracking_enabled(true);

-- An old provider has no scope ABI, but synthetic COPY storage continues to work.
SELECT test.teardown_tss_scope_hook_v1();
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- Invalid optional scope callbacks silently preserve legacy synthetic storage.
DO $$ BEGIN PERFORM test.setup_tss_scope_hook_invalid(0); END $$;
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

DO $$ BEGIN PERFORM test.setup_tss_scope_hook_invalid(1); END $$;
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

DO $$ BEGIN PERFORM test.setup_tss_scope_hook_invalid(2); END $$;
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- Complete v1 requires reset and exit even when COPY itself only enters/exits.
DO $$ BEGIN PERFORM test.setup_tss_scope_hook_invalid(3); END $$;
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

DO $$ BEGIN PERFORM test.setup_tss_scope_hook_invalid(4); END $$;
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- A provider with only the scope ABI has no synthetic statement to protect.
SELECT test.setup_tss_scope_hook_v1();
SELECT test.teardown_tss_hook_v1();
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- A legacy ABI version mismatch warns once, when the operation begins.
SELECT test.setup_tss_hook_v0();
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- Check all 64 version bits, not just the low 32 bits that match version 1.
SELECT test.setup_tss_hook_version(4294967297);
\set VERBOSITY default
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.
\set VERBOSITY terse

SELECT test.teardown_tss_hook_v1();
