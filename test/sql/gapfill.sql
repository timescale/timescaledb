-- Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
--
-- This file is licensed under the Apache License,
-- see LICENSE-APACHE at the top level directory.

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
