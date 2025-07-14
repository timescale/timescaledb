-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Show the defaults
SHOW timescaledb.cagg_processing_wal_batch_size;

\set VERBOSITY default
\set ON_ERROR_STOP 0
SET timescaledb.cagg_processing_wal_batch_size TO 100;
SET timescaledb.cagg_processing_wal_batch_size TO 10000001;
\set ON_ERROR_STOP 1
