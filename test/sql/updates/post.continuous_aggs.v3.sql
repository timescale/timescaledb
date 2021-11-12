-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir post.continuous_aggs.v2.sql

SELECT "time", count(*) from rename_cols GROUP BY 1;

--verify compression can be enabled
ALTER MATERIALIZED VIEW rename_cols SET ( timescaledb.compress='true');

SELECT "time", count(*) from rename_cols GROUP BY 1;
