-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION test.condition() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_test_utils_condition' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION test.int64_eq() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_test_utils_int64_eq' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION test.ptr_eq() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_test_utils_ptr_eq' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
CREATE OR REPLACE FUNCTION test.double_eq() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_test_utils_double_eq' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- We're testing that the test utils work and generate errors on
-- failing conditions
\set ON_ERROR_STOP 0
SELECT test.condition();
SELECT test.int64_eq();
SELECT test.ptr_eq();
SELECT test.double_eq();
\set ON_ERROR_STOP 1

-- Test debug points
--
\set ECHO all

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION debug_point_enable(TEXT)
RETURNS VOID
AS :MODULE_PATHNAME, 'ts_debug_point_enable'
LANGUAGE C VOLATILE STRICT;

CREATE OR REPLACE FUNCTION debug_point_release(TEXT)
RETURNS VOID
AS :MODULE_PATHNAME, 'ts_debug_point_release'
LANGUAGE C VOLATILE STRICT;

-- debug point already enabled
SELECT debug_point_enable('test_debug_point');
\set ON_ERROR_STOP 0
SELECT debug_point_enable('test_debug_point');
\set ON_ERROR_STOP 1
SELECT debug_point_release('test_debug_point');

-- debug point not enabled
\set ON_ERROR_STOP 0
SELECT debug_point_release('test_debug_point');
\set ON_ERROR_STOP 1

-- error injections
--
CREATE OR REPLACE FUNCTION test_error_injection(TEXT)
RETURNS VOID
AS :MODULE_PATHNAME, 'ts_test_error_injection'
LANGUAGE C VOLATILE STRICT;
SET ROLE :ROLE_DEFAULT_PERM_USER;

SELECT test_error_injection('test_error');

SELECT debug_point_enable('test_error');
\set ON_ERROR_STOP 0
SELECT test_error_injection('test_error');
\set ON_ERROR_STOP 1

SELECT debug_point_release('test_error');
SELECT test_error_injection('test_error');

-- Test Scanner
RESET ROLE;
CREATE OR REPLACE FUNCTION test.scanner() RETURNS VOID
    AS :MODULE_PATHNAME, 'ts_test_scanner' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Create two chunks to scan in the test
CREATE TABLE hyper (time timestamptz, temp float);
SELECT create_hypertable('hyper', 'time');
INSERT INTO hyper VALUES ('2021-01-01', 1.0), ('2022-01-01', 2.0);
SELECT test.scanner();
