-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Import setup file to data nodes.
\unset ECHO
\c data_node_1 :ROLE_SUPERUSER
set client_min_messages to error;
\ir include/dist_remote_error_setup.sql

\c data_node_2 :ROLE_SUPERUSER
set client_min_messages to error;
\ir include/dist_remote_error_setup.sql

\set sleepy_recv 1
\c data_node_3 :ROLE_SUPERUSER
set client_min_messages to error;
\ir include/dist_remote_error_setup.sql
\unset sleepy_recv

\c :TEST_DBNAME :ROLE_SUPERUSER
set client_min_messages to error;
\ir include/dist_remote_error_setup.sql
\set ECHO all

-- Disable SSL to get stable error output across versions. SSL adds some output
-- that changed in PG 14.
set timescaledb.debug_enable_ssl to off;

set client_min_messages to error;

-- A relatively big table on one data node
create table metrics_dist_remote_error(like metrics_dist);
select table_name from create_distributed_hypertable('metrics_dist_remote_error', 'time', 'device_id',
    data_nodes => '{"data_node_1"}');
insert into metrics_dist_remote_error select * from metrics_dist order by metrics_dist limit 20000;

-- The error messages vary wildly between the Postgres versions, dependent on
-- the particular behavior of libqp in this or that case. The purpose of this
-- test is not to solidify this accidental behavior, but to merely exercise the
-- error handling code to make sure it doesn't have fatal errors. Unfortunately,
-- there is no way to suppress error output from a psql script.
set client_min_messages to ERROR;

\set ON_ERROR_STOP off

set timescaledb.remote_data_fetcher = 'rowbyrow';

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(0, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(1, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(2, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(701, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(10000, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(16384, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(10000000, device_id)::int != 0;

-- We don't test fatal errors here, because PG versions before 14 are unable to
-- report them properly to the access node, so we get different errors in these
-- versions.

-- Now test the same with the cursor fetcher.
set timescaledb.remote_data_fetcher = 'cursor';

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(0, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(1, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(2, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(701, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(10000, device_id)::int != 0;

explain (analyze, verbose, costs off, timing off, summary off)
select 1 from metrics_dist_remote_error where ts_debug_shippable_error_after_n_rows(10000000, device_id)::int != 0;

-- Table with broken send for a data type.

create table metrics_dist_bs(like metrics_dist);

alter table metrics_dist_bs alter column v0 type bs;

select table_name from create_distributed_hypertable('metrics_dist_bs',
    'time', 'device_id');

set timescaledb.enable_connection_binary_data to off;
insert into metrics_dist_bs
    select * from metrics_dist_remote_error;
set timescaledb.enable_connection_binary_data to on;

explain (analyze, verbose, costs off, timing off, summary off)
select * from metrics_dist_bs;

drop table metrics_dist_bs;

-- Table with broken receive for a data type.
create table metrics_dist_br(like metrics_dist);

alter table metrics_dist_br alter column v0 type br;

select table_name from create_distributed_hypertable('metrics_dist_br',
    'time', 'device_id');

-- Test that INSERT and COPY fail on data nodes.
-- Note that we use the text format for the COPY input, so that the access node
-- doesn't call `recv` and fail by itself. It's going to use binary format for
-- transfer to data nodes regardless of the input format.
-- First, create the reference.
\copy (select * from metrics_dist_remote_error) to 'dist_remote_error.text' with (format text);

-- We have to test various interleavings of COPY and INSERT to check that
-- one can recover from connection failure states introduced by another.
\copy metrics_dist_br from 'dist_remote_error.text' with (format text);
\copy metrics_dist_br from 'dist_remote_error.text' with (format text);
insert into metrics_dist_br select * from metrics_dist_remote_error;
insert into metrics_dist_br select * from metrics_dist_remote_error;
\copy metrics_dist_br from 'dist_remote_error.text' with (format text);

drop table metrics_dist_br;

-- Table with sleepy receive for a data type, to improve coverage of the waiting
-- code on the access node.
create table metrics_dist_bl(like metrics_dist);

alter table metrics_dist_bl alter column v0 type bl;

select table_name from create_distributed_hypertable('metrics_dist_bl',
    'time', 'device_id');

-- Test INSERT and COPY with slow data node.
\copy metrics_dist_bl from 'dist_remote_error.text' with (format text);

insert into metrics_dist_bl select * from metrics_dist_remote_error;

select count(*) from metrics_dist_bl;

drop table metrics_dist_bl;

drop table metrics_dist_remote_error;
