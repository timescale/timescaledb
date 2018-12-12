-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Timescale License,
-- see LICENSE-TIMESCALE at the top of the tsl directory.

SELECT _timescaledb_internal.current_license_key();
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();

-- changing licenses requires superuser privleges
\set ON_ERROR_STOP 0
SET timescaledb.license_key='CommunityLicense';
SELECT _timescaledb_internal.current_license_key();
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();
\set ON_ERROR_STOP 1

\c single :ROLE_SUPERUSER
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

SET timescaledb.license_key=Default;
SELECT _timescaledb_internal.current_license_key();
SELECT _timescaledb_internal.tsl_loaded();
SELECT _timescaledb_internal.enterprise_enabled();

\c single :ROLE_DEFAULT_PERM_USER
