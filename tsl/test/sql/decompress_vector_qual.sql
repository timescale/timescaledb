-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

create table vectorqual(metric1 int8, ts timestamp, metric2 int8, device int8);
select create_hypertable('vectorqual', 'ts');
alter table vectorqual set (timescaledb.compress, timescaledb.compress_segmentby = 'device');

insert into vectorqual(ts, device, metric1, metric2) values ('2020-01-01 00:00:00', 1, 11, 12);
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

alter table vectorqual drop column metric1;
insert into vectorqual(ts, device, metric2) values ('2021-01-01 00:00:00', 2, 22);
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

alter table vectorqual add column metric3 int4 default 777;
insert into vectorqual(ts, device, metric2, metric3) values ('2022-01-01 00:00:00', 3, 32, 33);
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

alter table vectorqual add column metric4 int8;
insert into vectorqual(ts, device, metric2, metric3, metric4) values ('2023-01-01 00:00:00', 4, 42, 43, 44);
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

select * from vectorqual order by vectorqual;

set timescaledb.debug_require_vector_qual to 'only' /* all following quals must be vectorized */;
select count(*) from vectorqual where ts > '1999-01-01 00:00:00';
select count(*) from vectorqual where metric2 = 22;
select count(*) from vectorqual where 22 = metric2 /* commutators */;
select count(*) from vectorqual where metric3 = 33;
select count(*) from vectorqual where metric3 = 777 /* default value */;
select count(*) from vectorqual where metric4 = 44 /* column with default null */;
select count(*) from vectorqual where metric4 >= 0 /* nulls shouldn't pass the qual */;

set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where device = 1 /* can't apply vector ops to the segmentby column */;


-- Test columns that don't support bulk decompression.
alter table vectorqual add column tag text;
insert into vectorqual(ts, device, metric2, metric3, metric4, tag) values ('2025-01-01 00:00:00', 5, 52, 53, 54, 'tag5');
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

set timescaledb.debug_require_vector_qual to 'only';
select tag from vectorqual where metric2 > 0;


-- Queries without aggregation.
select * from vectorqual where ts > '2021-01-01 00:00:00' order by vectorqual;
select * from vectorqual where metric4 >= 0 order by vectorqual;


-- Constraints on columns not selected.
select metric4 from vectorqual where ts > '2021-01-01 00:00:00' order by 1;


-- ANDed constraints on multiple columns.
select * from vectorqual where ts > '2021-01-01 00:00:00' and metric3 > 40 order by vectorqual;


-- ORed constrainst on multiple columns (not vectorized for now).
set timescaledb.debug_require_vector_qual to 'forbid';
select * from vectorqual where ts > '2021-01-01 00:00:00' or metric3 > 40 order by vectorqual;


-- Test with unary operator.
create operator !! (function = 'bool', rightarg = int4);
select count(*) from vectorqual where !!metric3;


-- Custom operator on column that supports bulk decompression is not vectorized.
set timescaledb.debug_require_vector_qual to 'forbid';
create function int4eqq(int4, int4) returns bool as 'int4eq' language internal;
create operator === (function = 'int4eqq', rightarg = int4, leftarg = int4);
select count(*) from vectorqual where metric3 === 777;
select count(*) from vectorqual where metric3 === any(array[777, 888]);


-- NullTest is not vectorized.
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where metric4 is null;
select count(*) from vectorqual where metric4 is not null;


-- Scalar array operators are vectorized if the operator is vectorizable.
set timescaledb.debug_require_vector_qual to 'only';
select count(*) from vectorqual where metric3 = any(array[777, 888]); /* default value */
select count(*) from vectorqual where metric4 = any(array[44, 55]) /* default null */;
select count(*) from vectorqual where metric2 > any(array[-1, -2, -3]) /* any */;
select count(*) from vectorqual where metric2 > all(array[-1, -2, -3]) /* all */;

-- Also have to support null array elements, because they are impossible to
-- prevent in stable expressions.
set timescaledb.debug_require_vector_qual to 'only';
select count(*) from vectorqual where metric2 = any(array[null::int]) /* any with null element */;
select count(*) from vectorqual where metric2 = any(array[22, null]) /* any with null element */;
select count(*) from vectorqual where metric2 = any(array[null, 32]) /* any with null element */;
select count(*) from vectorqual where metric2 = any(array[22, null, 32]) /* any with null element */;
select count(*) from vectorqual where metric2 = all(array[null::int]) /* all with null element */;
select count(*) from vectorqual where metric2 = all(array[22, null]) /* all with null element */;
select count(*) from vectorqual where metric2 = all(array[null, 32]) /* all with null element */;
select count(*) from vectorqual where metric2 = all(array[22, null, 32]) /* all with null element */;

-- Check early exit.
reset timescaledb.debug_require_vector_qual;
create table singlebatch(like vectorqual);
select create_hypertable('singlebatch', 'ts');
alter table singlebatch set (timescaledb.compress);
insert into singlebatch select '2022-02-02 02:02:02', metric2, device, metric3, metric4, tag from vectorqual;
select count(compress_chunk(x, true)) from show_chunks('singlebatch') x;

set timescaledb.debug_require_vector_qual to 'only';
select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 22]);
select count(*) from singlebatch where metric2 = any(array[0, 22, 0, 0, 0]);
select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 0]);
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 0]);
select count(*) from singlebatch where metric2 <= all(array[12, 0, 12, 12, 12]);
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 12]);

select count(*) from singlebatch where metric3 = 777 and metric2 = any(array[0, 0, 0, 0, 22]);
select count(*) from singlebatch where metric3 = 777 and metric2 = any(array[0, 22, 0, 0, 0]);
select count(*) from singlebatch where metric3 = 777 and metric2 = any(array[0, 0, 0, 0, 0]);
select count(*) from singlebatch where metric3 = 777 and metric2 <= all(array[12, 12, 12, 12, 0]);
select count(*) from singlebatch where metric3 = 777 and metric2 <= all(array[12, 0, 12, 12, 12]);
select count(*) from singlebatch where metric3 = 777 and metric2 <= all(array[12, 12, 12, 12, 12]);

select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 22]) and metric3 = 777;
select count(*) from singlebatch where metric2 = any(array[0, 22, 0, 0, 0]) and metric3 = 777;
select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 0]) and metric3 = 777;
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 0]) and metric3 = 777;
select count(*) from singlebatch where metric2 <= all(array[12, 0, 12, 12, 12]) and metric3 = 777;
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 12]) and metric3 = 777;

select count(*) from singlebatch where metric3 != 777 and metric2 = any(array[0, 0, 0, 0, 22]);
select count(*) from singlebatch where metric3 != 777 and metric2 = any(array[0, 22, 0, 0, 0]);
select count(*) from singlebatch where metric3 != 777 and metric2 = any(array[0, 0, 0, 0, 0]);
select count(*) from singlebatch where metric3 != 777 and metric2 <= all(array[12, 12, 12, 12, 0]);
select count(*) from singlebatch where metric3 != 777 and metric2 <= all(array[12, 0, 12, 12, 12]);
select count(*) from singlebatch where metric3 != 777 and metric2 <= all(array[12, 12, 12, 12, 12]);

select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 22]) and metric3 != 777;
select count(*) from singlebatch where metric2 = any(array[0, 22, 0, 0, 0]) and metric3 != 777;
select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 0]) and metric3 != 777;
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 0]) and metric3 != 777;
select count(*) from singlebatch where metric2 <= all(array[12, 0, 12, 12, 12]) and metric3 != 777;
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 12]) and metric3 != 777;



-- Comparison with other column not vectorized.
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where metric3 = metric4;
select count(*) from vectorqual where metric3 = any(array[metric4]);


-- Vectorized filters also work if we have only stable functions on the right
-- side that can be evaluated to a constant at run time.
set timescaledb.debug_require_vector_qual to 'only';
select count(*) from vectorqual where ts > '2021-01-01 00:00:00'::timestamptz::timestamp;
select count(*) from vectorqual where ts > '2021-01-01 00:00:00'::timestamp - interval '1 day';
-- Expression that evaluates to Null.
select count(*) from vectorqual where ts > case when '2021-01-01'::timestamp < '2022-01-01'::timestamptz then null else '2021-01-01 00:00:00'::timestamp end;

-- This filter is not vectorized because the 'timestamp > timestamptz'
-- operator is stable, not immutable, because it uses the current session
-- timezone. We could transform it to something like
-- 'timestamp > timestamptz::timestamp' to allow our stable function evaluation
-- to handle this case, but we don't do it at the moment.
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where ts > '2021-01-01 00:00:00'::timestamptz;

-- Can't vectorize comparison with a volatile function.
select count(*) from vectorqual where metric3 > random()::int - 100;
select count(*) from vectorqual where ts > case when random() < 10 then null else '2021-01-01 00:00:00'::timestamp end;


-- Test that the vectorized quals are disabled by disabling the bulk decompression.
set timescaledb.enable_bulk_decompression to off;
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where metric4 > null;
set timescaledb.enable_bulk_decompression to on;


-- Test that the debug GUC works
\set ON_ERROR_STOP 0
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where metric4 > 4;
set timescaledb.debug_require_vector_qual to 'only';
select count(*) from vectorqual where metric4 is null;
\set ON_ERROR_STOP 1


-- Date columns
create table date_table(ts date);
select create_hypertable('date_table', 'ts');
alter table date_table set (timescaledb.compress);
insert into date_table values ('2021-01-01'), ('2021-01-02'),
    ('2021-01-03');
select count(compress_chunk(x, true)) from show_chunks('date_table') x;

set timescaledb.debug_require_vector_qual to 'only';
select * from date_table where ts >  '2021-01-02';
select * from date_table where ts >= '2021-01-02';
select * from date_table where ts =  '2021-01-02';
select * from date_table where ts <= '2021-01-02';
select * from date_table where ts <  '2021-01-02';
