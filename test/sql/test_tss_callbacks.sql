-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE OR REPLACE FUNCTION test.setup_tss_hook_v0() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_setup_tss_hook_v0' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.setup_tss_hook_v1() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_setup_tss_hook_v1' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.teardown_tss_hook_v1() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_teardown_tss_hook_v1' LANGUAGE C VOLATILE;

SELECT test.setup_tss_hook_v1();

CREATE TABLE copy_test (
    "time" timestamptz NOT NULL,
    "value" double precision NOT NULL
);

SELECT create_hypertable('copy_test', 'time');

-- We should se a mock message
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

SELECT test.teardown_tss_hook_v1();

-- Without the hook registered we cannot see the mock message
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.

-- Test for mismatch version
SELECT test.setup_tss_hook_v0();

-- Warning because the mismatch versions
COPY copy_test FROM STDIN DELIMITER ',';
2020-01-01 01:10:00+01,1
2021-01-01 01:10:00+01,1
\.
