-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_debug.extension_state();

SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
SELECT _timescaledb_debug.extension_state();
\set ON_ERROR_STOP 1
