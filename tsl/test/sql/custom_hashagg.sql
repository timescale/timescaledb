-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table foo( a integer, b timestamptz);
select count(*) from create_hypertable('foo', 'b');

insert into foo values
       (1, '2004-10-19 10:23:54'),
       (1, '2005-10-19 10:23:54'),
       (1, '2005-01-01 00:00:00+00'),
       (2, '2005-01-01 00:00:00+00');

-- Test that the range estimation functions estimate_max_spread_var()
-- is used for custom hash aggregates and that they behave in a sane
-- manner when there are errors.
--
-- To trigger a call to the function, the following are required:
--
-- timescaledb.enable_custom_hashagg to be true
--
-- query should either date_trunc or time_bucket bucket function
-- with a recognized time type
--
-- an aggregation function in the result
--
-- a group-by on the bucket created by the date_trunc or time_bucket
--
-- statistics recorded for the variable used in the bucketing
-- function

set timescaledb.enable_custom_hashagg to true;
analyze foo;
explain (costs off)
select date_trunc('hour', b) bucket, sum(a) from foo group by bucket;
select date_trunc('hour', b) bucket, sum(a) from foo group by bucket;

-- Inserting a very large value should trigger an error inside the
-- range estimation function estimate_max_spread_var() and test that
-- it works even in the presence of errors.
insert into foo values
       (99, 'epoch'::timestamptz + '9223371331200000000'::bigint * '1 microsecond'::interval);
\set ON_ERROR_STOP 0
select date_trunc('hour', b) bucket, sum(a) from foo group by bucket;
\set ON_ERROR_STOP 1
delete from foo where a = 99;
reset timescaledb.enable_custom_hashagg;
