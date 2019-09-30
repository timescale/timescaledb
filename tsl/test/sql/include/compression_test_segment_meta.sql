-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors

SELECT
     _timescaledb_internal.segment_meta_min_max_agg_max(i) = max(i),
     _timescaledb_internal.segment_meta_min_max_agg_min(i) = min(i)
FROM :"TABLE";

\set ECHO all
