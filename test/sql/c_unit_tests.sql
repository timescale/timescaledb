-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION ts_test_time_to_internal_conversion() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_test_interval_to_internal_conversion() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;

CREATE OR REPLACE FUNCTION ts_test_adts() RETURNS VOID
AS :MODULE_PATHNAME LANGUAGE C VOLATILE;
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

SELECT ts_test_time_to_internal_conversion();

SELECT ts_test_interval_to_internal_conversion();

SELECT ts_test_adts();
