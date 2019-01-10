-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

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
