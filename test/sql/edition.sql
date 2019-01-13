-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SELECT _timescaledb_internal.current_license_key();
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();

\unset ECHO
\o /dev/null
\ir include/test_utils.sql
\o
\set ECHO queries
\set VERBOSITY default

\c :TEST_DBNAME :ROLE_SUPERUSER

SELECT allow_downgrade_to_apache();
SET timescaledb.license_key='ApacheOnly';
select * from timescaledb_information.license;
