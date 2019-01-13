-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\unset ECHO
\o /dev/null
\ir include/test_utils.sql
\o
\set ECHO queries
\set VERBOSITY default

\c :TEST_DBNAME :ROLE_SUPERUSER

SELECT allow_downgrade_to_apache();
SET timescaledb.license_key='ApacheOnly';

\set ON_ERROR_STOP 0
SELECT locf(1);
SELECT interpolate(1);
SELECT time_bucket_gapfill(1,1,1,1);
\set ON_ERROR_STOP 1
