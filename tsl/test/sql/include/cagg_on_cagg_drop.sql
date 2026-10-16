-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- DROP of a 3 level hierarchy of CAGGs on CAGGs, materialized_only
-- taken from MAT_ONLY_1ST, MAT_ONLY_2TH and MAT_ONLY_3TH
CREATE TABLE drop_raw (time TIMESTAMPTZ NOT NULL, device_id INT, value FLOAT);
SELECT table_name FROM create_hypertable('drop_raw', 'time');

-- Count what is left of the caggs with the given materialization hypertables
CREATE FUNCTION cagg_drop_leftovers(mat_ids INT[])
RETURNS TABLE (caggs BIGINT, hypertables BIGINT, internal_views BIGINT, jobs BIGINT)
LANGUAGE SQL AS $$
  SELECT
    (SELECT count(*) FROM _timescaledb_catalog.continuous_agg WHERE mat_hypertable_id = ANY(mat_ids)),
    (SELECT count(*) FROM _timescaledb_catalog.hypertable WHERE id = ANY(mat_ids)),
    (SELECT count(*) FROM pg_class
      WHERE relname IN (SELECT format('_%s_view_%s', kind, id)
                        FROM unnest(mat_ids) id, unnest(ARRAY['partial', 'direct']) kind)),
    (SELECT count(*) FROM _timescaledb_config.bgw_job WHERE hypertable_id = ANY(mat_ids));
$$;

\ir cagg_on_cagg_drop_setup.sql

-- RESTRICT should error naming the child cagg
\set VERBOSITY default
\set ON_ERROR_STOP 0
DROP MATERIALIZED VIEW drop_cagg_1;
DROP MATERIALIZED VIEW drop_cagg_2;
\set ON_ERROR_STOP 1
\set VERBOSITY terse
SELECT * FROM cagg_drop_leftovers(:'mat_ids');

-- RESTRICT should error even when the children are dropped by the same statement,
-- since their internal views still depend on the parent
\set ON_ERROR_STOP 0
DROP MATERIALIZED VIEW drop_cagg_3, drop_cagg_2, drop_cagg_1;
\set ON_ERROR_STOP 1
SELECT * FROM cagg_drop_leftovers(:'mat_ids');

-- CASCADE from the middle level should not affect the parent
DROP MATERIALIZED VIEW drop_cagg_2 CASCADE;
SELECT user_view_name FROM _timescaledb_catalog.continuous_agg WHERE user_view_name LIKE 'drop_cagg_%';
DROP MATERIALIZED VIEW drop_cagg_1;
SELECT * FROM cagg_drop_leftovers(:'mat_ids');

-- CASCADE from the top level should drop the whole hierarchy
\ir cagg_on_cagg_drop_setup.sql
DROP MATERIALIZED VIEW drop_cagg_1 CASCADE;
SELECT * FROM cagg_drop_leftovers(:'mat_ids');

-- Dropping the raw hypertable should drop the whole hierarchy
\ir cagg_on_cagg_drop_setup.sql
\set ON_ERROR_STOP 0
DROP TABLE drop_raw;
\set ON_ERROR_STOP 1
DROP TABLE drop_raw CASCADE;
SELECT * FROM cagg_drop_leftovers(:'mat_ids');

DROP FUNCTION cagg_drop_leftovers(INT[]);
