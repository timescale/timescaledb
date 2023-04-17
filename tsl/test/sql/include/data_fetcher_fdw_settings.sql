-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Default settings
EXPLAIN (COSTS) SELECT * FROM one_batch;

-- Set custom startup cost
ALTER FOREIGN DATA WRAPPER timescaledb_fdw OPTIONS (ADD fdw_startup_cost '200');
EXPLAIN (COSTS) SELECT * FROM one_batch;

-- Set custom tuple cost
ALTER FOREIGN DATA WRAPPER timescaledb_fdw OPTIONS (ADD fdw_tuple_cost '2');
EXPLAIN (COSTS) SELECT * FROM one_batch;

-- Update startup cost
ALTER FOREIGN DATA WRAPPER timescaledb_fdw OPTIONS (SET fdw_startup_cost '2');
EXPLAIN (COSTS) SELECT * FROM one_batch;

-- Update startup cost
ALTER FOREIGN DATA WRAPPER timescaledb_fdw OPTIONS (SET fdw_tuple_cost '0.5');
EXPLAIN (COSTS) SELECT * FROM one_batch;

-- Reset custom settings
ALTER FOREIGN DATA WRAPPER timescaledb_fdw OPTIONS (DROP fdw_startup_cost);
ALTER FOREIGN DATA WRAPPER timescaledb_fdw OPTIONS (DROP fdw_tuple_cost);
EXPLAIN (COSTS) SELECT * FROM one_batch;

