-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

SET timescaledb.disable_optimizations= 'off';
\set PREFIX 'EXPLAIN (costs off) '
\ir include/plan_expand_hypertable_load.sql
\ir include/plan_expand_hypertable_query.sql
\ir include/plan_expand_hypertable_chunks_in_query.sql
