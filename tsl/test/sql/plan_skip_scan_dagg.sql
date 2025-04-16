-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- need superuser to modify statistics
\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER
\ir include/skip_scan_load.sql

-- we want to run with analyze here so we can see counts in the nodes
\set PREFIX 'EXPLAIN (analyze, costs off, timing off, summary off)'
\set TABLE skip_scan
\ir include/skip_scan_dagg_query.sql

\set TABLE skip_scan_ht
\ir include/skip_scan_dagg_query.sql
\ir include/skip_scan_dagg_query_ht.sql

-- For compressed SkipScan, we can only use compressed index on segmentby column "skip_scan_htc.dev",
-- meaning indexes not matching it in the below tests will not use SkipScan.
\set TABLE skip_scan_htc
\ir include/skip_scan_dagg_query.sql


