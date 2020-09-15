-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir test-rerun.v1.sql

SELECT count(*) FROM mat_inval;
REFRESH MATERIALIZED VIEW mat_inval;
SELECT count(*) FROM mat_inval;
