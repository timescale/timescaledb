-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timezone TO PST8PDT;

\set invalidate_using trigger
\ir include/cagg_refresh_common.sql
