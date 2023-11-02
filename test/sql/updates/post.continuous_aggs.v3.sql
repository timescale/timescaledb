-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir post.continuous_aggs.v2.sql

-- Ensure CAgg is refreshed
SELECT
	extversion < '2.0.0' AS has_refresh_mat_view
  FROM pg_extension
 WHERE extname = 'timescaledb' \gset

\if :has_refresh_mat_view
REFRESH MATERIALIZED VIEW rename_cols;
\else
CALL refresh_continuous_aggregate('rename_cols',NULL,NULL);
\endif

SELECT "time", count(*) from rename_cols GROUP BY 1 ORDER BY 1;

--verify compression can be enabled
ALTER MATERIALIZED VIEW rename_cols SET ( timescaledb.compress='true');

SELECT "time", count(*) from rename_cols GROUP BY 1 ORDER BY 1;
