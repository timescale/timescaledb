-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

-- ########################################################
-- ## INTEGER data type tests
-- ########################################################
\set IS_TIME_DIMENSION FALSE
\set TIME_DIMENSION_DATATYPE INTEGER
\ir include/cagg_migrate_common.sql

-- ########################################################
-- ## TIMESTAMP data type tests
-- ########################################################
\set IS_TIME_DIMENSION TRUE
\set TIME_DIMENSION_DATATYPE TIMESTAMP
\ir include/cagg_migrate_common.sql

-- ########################################################
-- ## TIMESTAMPTZ data type tests
-- ########################################################
\set IS_TIME_DIMENSION TRUE
\set TIME_DIMENSION_DATATYPE TIMESTAMPTZ
\ir include/cagg_migrate_common.sql


-- #########################################################
-- Issue 5359 - custom timezones should not break the migration
-- #########################################################
SET timezone = 'Europe/Budapest';

-- Test with timestamp
\set TIME_DIMENSION_DATATYPE TIMESTAMP
\ir include/cagg_migrate_custom_timezone.sql

-- Test with timestamptz
\set TIME_DIMENSION_DATATYPE TIMESTAMPTZ
\ir include/cagg_migrate_custom_timezone.sql
