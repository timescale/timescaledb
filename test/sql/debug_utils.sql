-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_debug.extension_state();

SET ROLE :ROLE_DEFAULT_PERM_USER;

\set ON_ERROR_STOP 0
SELECT _timescaledb_debug.extension_state();
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_SUPERUSER
DO $$
DECLARE
    module text;
BEGIN
    SELECT probin INTO module FROM pg_proc WHERE proname = 'extension_state' AND pronamespace = '_timescaledb_debug'::regnamespace;
    EXECUTE format('CREATE FUNCTION extension_state() RETURNS TEXT AS ''%s'', ''ts_extension_get_state'' LANGUAGE C', module);
END
$$;

DROP EXTENSION timescaledb;
SELECT * FROM extension_state();

\c
CREATE EXTENSION timescaledb;
SELECT * FROM extension_state();
