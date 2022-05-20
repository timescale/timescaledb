-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER;

-- A relatively big table on one data node
CREATE TABLE metrics_dist_remote_error(LIKE metrics_dist);
SELECT table_name FROM create_distributed_hypertable('metrics_dist_remote_error', 'time', 'device_id',
    data_nodes => '{"data_node_1"}');
INSERT INTO metrics_dist_remote_error SELECT * FROM metrics_dist ORDER BY random() LIMIT 20000;

-- Create a function that raises an error every nth row.
-- It's stable, takes a second argument and returns current number of rows,
-- so that it is shipped to data nodes and not optimized out.
-- It's written in one line because I don't know how to make \set accept
-- several lines.
\set error_function 'CREATE OR REPLACE FUNCTION ts_debug_shippable_error_after_n_rows(integer, anyelement) RETURNS integer AS ':MODULE_PATHNAME', ''ts_debug_shippable_error_after_n_rows'' LANGUAGE C STABLE STRICT; '
-- Same as above, but fatal.
\set fatal_function 'CREATE OR REPLACE FUNCTION ts_debug_shippable_fatal_after_n_rows(integer, anyelement) RETURNS integer AS ':MODULE_PATHNAME', ''ts_debug_shippable_error_after_n_rows'' LANGUAGE C STABLE STRICT; '

:error_function
call distributed_exec(:'error_function');
:fatal_function
call distributed_exec(:'fatal_function');

-- The error messages vary wildly between the Postgres versions, dependent on
-- the particular behavior of libqp in this or that case. The purpose of this
-- test is not to solidify this accidental behavior, but to merely exercise the
-- error handling code to make sure it doesn't have fatal errors. Unfortunately,
-- there is no way to suppress error output from a psql script.
set client_min_messages to ERROR;

\set ON_ERROR_STOP off

set timescaledb.remote_data_fetcher = 'rowbyrow';

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(1, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(10000, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(10000000, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_fatal_after_n_rows(1, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_fatal_after_n_rows(10000, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_fatal_after_n_rows(10000000, device_id)::int != 0;

set timescaledb.remote_data_fetcher = 'cursor';

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(1, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(10000, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(10000000, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_fatal_after_n_rows(1, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_fatal_after_n_rows(10000, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_fatal_after_n_rows(10000000, device_id)::int != 0;

DROP TABLE metrics_dist_remote_error;

