-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set IS_DISTRIBUTED FALSE
\set IS_TIME_DIMENSION FALSE

-- ########################################################
-- ## INTEGER data type tests
-- ########################################################
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
