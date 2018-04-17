SET timescaledb.disable_optimizations= 'off';
\set PREFIX 'EXPLAIN (costs off) '
\ir include/plan_expand_hypertable_load.sql
\ir include/plan_expand_hypertable_query.sql
