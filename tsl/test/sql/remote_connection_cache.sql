-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE FUNCTION _timescaledb_internal.test_remote_connection_cache()
RETURNS void
AS :TSL_MODULE_PATHNAME, 'tsl_test_remote_connection_cache'
LANGUAGE C STRICT;

DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER loopback_1 FOREIGN DATA WRAPPER timescaledb_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
        EXECUTE $$CREATE SERVER loopback_2 FOREIGN DATA WRAPPER timescaledb_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;

CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_1;
CREATE USER MAPPING FOR CURRENT_USER SERVER loopback_2;

SELECT _timescaledb_internal.test_remote_connection_cache();
