-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();
select * from timescaledb_information.license;

-- changing licenses requires superuser privleges
\set ON_ERROR_STOP 0
SET timescaledb.license_key='CommunityLicense';
SELECT _timescaledb_internal.current_license_key();
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();
\set ON_ERROR_STOP 1

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_internal.current_license_key();
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();

\set ON_ERROR_STOP 0
SET timescaledb.license_key='ApacheOnly';
SELECT _timescaledb_internal.current_license_key();
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();
\set ON_ERROR_STOP 1


SET timescaledb.license_key='CommunityLicense';
SELECT _timescaledb_internal.current_license_key();
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();
select * from timescaledb_information.license;

SET timescaledb.license_key=Default;
SELECT _timescaledb_internal.current_license_key();
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();
select * from timescaledb_information.license;
