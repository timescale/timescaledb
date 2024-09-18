-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Connect as superuser to use SET ROLE later
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

SET timezone TO PST8PDT;

-- Run tests with default role
SET ROLE :ROLE_DEFAULT_PERM_USER;

-- Enable MERGE statements for continuous aggregate refresh
SET timescaledb.enable_merge_on_cagg_refresh TO ON;

\set TEST_BASE_NAME cagg_query_using_merge
\ir include/cagg_query_common.sql
