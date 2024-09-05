-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Enable MERGE statements for continuous aggregate refresh
SET timescaledb.enable_merge_on_cagg_refresh TO ON;

\ir include/cagg_refresh_common.sql
