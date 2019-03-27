-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SET timescaledb.disable_optimizations= 'on';
SET max_parallel_workers_per_gather = 0; -- Disable parallel for this test
\ir include/sql_query_results.sql
RESET max_parallel_workers_per_gather;
