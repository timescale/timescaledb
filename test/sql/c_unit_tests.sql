-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION test.time_to_internal_conversion() RETURNS VOID
AS :MODULE_PATHNAME, 'ts_test_time_to_internal_conversion' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.interval_to_internal_conversion() RETURNS VOID
AS :MODULE_PATHNAME, 'ts_test_interval_to_internal_conversion' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.adts() RETURNS VOID
AS :MODULE_PATHNAME, 'ts_test_adts' LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION test.time_utils() RETURNS VOID
AS :MODULE_PATHNAME, 'ts_test_time_utils' LANGUAGE C;
SET ROLE :ROLE_DEFAULT_PERM_USER;

SELECT test.time_to_internal_conversion();
SELECT test.interval_to_internal_conversion();
SELECT test.adts();
SELECT test.time_utils();
