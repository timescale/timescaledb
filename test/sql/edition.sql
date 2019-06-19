-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.


\c :TEST_DBNAME :ROLE_SUPERUSER

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

\set ON_ERROR_STOP 0
SET timescaledb.license_key='ApacheOnly';
\set ON_ERROR_STOP 1

SELECT allow_downgrade_to_apache();
SET timescaledb.license_key='ApacheOnly';
select * from timescaledb_information.license;

SELECT _timescaledb_internal.current_db_set_license_key('CommunityLicense');
select * from timescaledb_information.license;

-- Default perm user shouldn't be able to change the license key.
\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- Hides error messages in cases where error messages differe between Postgres versions
create or replace function get_sqlstate(in_text TEXT) RETURNS TEXT AS
$$
BEGIN
    BEGIN
        EXECUTE in_text;
    EXCEPTION WHEN others THEN GET STACKED DIAGNOSTICS in_text = RETURNED_SQLSTATE;
    END;
    RETURN in_text;
END;
$$
LANGUAGE PLPGSQL;

--allowed
SELECT * FROM timescaledb_information.license;
SELECT * FROM  _timescaledb_internal.enterprise_enabled();
SELECT * FROM  _timescaledb_internal.tsl_loaded();
SELECT * FROM  _timescaledb_internal.license_expiration_time();
SELECT * FROM  _timescaledb_internal.print_license_expiration_info();
SELECT * FROM  _timescaledb_internal.license_edition();


--disallowd
\set ON_ERROR_STOP 0
SELECT get_sqlstate($$SELECT _timescaledb_internal.current_db_set_license_key('ApacheOnly')$$);
SELECT get_sqlstate($$SHOW timescaledb.license_key;$$);
SELECT * FROM  _timescaledb_internal.current_license_key();
\set ON_ERROR_STOP 1
drop function get_sqlstate(TEXT);
