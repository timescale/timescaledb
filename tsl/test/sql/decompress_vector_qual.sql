-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

create function stable_abs(x int4) returns int4 as 'int4abs' language internal stable;

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

-- single chunk
select * from vectorqual where ts between '2019-02-02' and '2020-02-02' order by vectorqual;
select * from vectorqual where ts between '2020-02-02' and '2021-02-02' order by vectorqual;
select * from vectorqual where ts between '2021-02-02' and '2022-02-02' order by vectorqual;
select * from vectorqual where ts between '2022-02-02' and '2023-02-02' order by vectorqual;

set timescaledb.debug_require_vector_qual to 'require' /* all following quals must be vectorized */;
select count(*) from vectorqual where ts > '1999-01-01 00:00:00';
select count(*) from vectorqual where metric2 = 22;
select count(*) from vectorqual where 22 = metric2 /* commutators */;
select count(*) from vectorqual where metric3 = 33;
select count(*) from vectorqual where metric3 = 777 /* default value */;
select count(*) from vectorqual where metric4 = 44 /* column with default null */;
select count(*) from vectorqual where metric4 >= 0 /* nulls shouldn't pass the qual */;

set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where device = 1 /* can't apply vector ops to the segmentby column */;


-- Test various combinations of arithmetic types.
create table arithmetic(ts int, a int2, b int4, c int8, d float4, e float8,
    ax int2, bx int4, cx int8, dx float4, ex float8);
select create_hypertable('arithmetic', 'ts');
alter table arithmetic set (timescaledb.compress);
insert into arithmetic values (100, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
    (101, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);
select count(compress_chunk(x, true)) from show_chunks('arithmetic') x;
set timescaledb.debug_require_vector_qual to 'require';
select * from arithmetic where
        a > 1::int2 and a > 1::int4 and a > 1::int8
    and b > 1::int2 and b > 1::int4 and b > 1::int8
    and c > 1::int2 and c > 1::int4 and c > 1::int8
    and d > 1::int2 and d > 1::int4 and d > 1::int8 and d > 1::float4 and d > 1::float8
    and e > 1::int2 and e > 1::int4 and e > 1::int8 and e > 1::float4 and e > 1::float8
;


-- Test columns that don't support bulk decompression.
alter table vectorqual add column tag name;
insert into vectorqual(ts, device, metric2, metric3, metric4, tag) values ('2025-01-01 00:00:00', 5, 52, 53, 54, 'tag5');
select count(compress_chunk(x, true)) from show_chunks('vectorqual') x;

set timescaledb.debug_require_vector_qual to 'require';
select tag from vectorqual where metric2 > 0;


-- Can't vectorize parameterized join clauses for now.
set timescaledb.debug_require_vector_qual to 'forbid';
set enable_hashjoin to off;
set enable_mergejoin to off;
with values(x) as materialized(select distinct metric2 from vectorqual)
    select x, (select metric2 from vectorqual where metric2 = x) from values order by 1;
reset enable_hashjoin;
reset enable_mergejoin;

-- Can't vectorize initplan parameters either.
select count(*) from vectorqual where metric2
    = (select metric2 from vectorqual order by 1 limit 1);

-- Can vectorize clauses with query parameters.
set timescaledb.debug_require_vector_qual to 'require';
set plan_cache_mode to 'force_generic_plan';

prepare p as select count(*) from vectorqual where metric3 = $1;
execute p(33);
deallocate p;

-- Also try query parameter in combination with a stable function.
prepare p(int4) as select count(*) from vectorqual where metric3 = stable_abs($1);
execute p(33);
deallocate p;

reset plan_cache_mode;


-- Queries without aggregation.
set timescaledb.debug_require_vector_qual to 'require';
select * from vectorqual where ts > '2021-01-01 00:00:00' order by vectorqual;
select * from vectorqual where metric4 >= 0 order by vectorqual;


-- Constraints on columns not selected.
select metric4 from vectorqual where ts > '2021-01-01 00:00:00' order by 1;


-- ANDed constraints on multiple columns.
select * from vectorqual where ts > '2021-01-01 00:00:00' and metric3 > 40 order by vectorqual;


-- ORed constrainst on multiple columns.
set timescaledb.debug_require_vector_qual to 'require';
-- set timescaledb.debug_require_vector_qual to 'forbid';
-- set timescaledb.enable_bulk_decompression to off;

select * from vectorqual where ts > '2021-01-01 00:00:00' or metric3 > 40 order by vectorqual;

-- Some more tests for boolean operations.
select count(*) from vectorqual where ts > '2021-01-01 00:00:00';

select count(*) from vectorqual where 40 < metric3;

select count(*) from vectorqual where metric2 < 0;

select count(*) from vectorqual where ts > '2021-01-01 00:00:00' or 40 < metric3;

select count(*) from vectorqual where not (ts <= '2021-01-01 00:00:00' and 40 >= metric3);

-- early exit inside AND BoolExpr
select count(*) from vectorqual where metric2 < 0 or (metric4 < -1 and 40 >= metric3);

-- early exit after OR BoolExpr
select count(*) from vectorqual where metric2 < 0 or metric3  < -1;

-- expression evaluated to null at run time
select count(*) from vectorqual where metric3 = 777
    or metric3 > case when now() > now() - interval '1s' then null else 1 end;

reset timescaledb.enable_bulk_decompression;


-- Test with unary operator.
set timescaledb.debug_require_vector_qual to 'forbid';
create operator !! (function = 'bool', rightarg = int4);
select count(*) from vectorqual where !!metric3;
select count(*) from vectorqual where not !!metric3;


-- Custom operator on column that supports bulk decompression is not vectorized.
set timescaledb.debug_require_vector_qual to 'forbid';
create function int4eqq(int4, int4) returns bool as 'int4eq' language internal;
create operator === (function = 'int4eqq', rightarg = int4, leftarg = int4);
select count(*) from vectorqual where metric3 === 777;
select count(*) from vectorqual where metric3 === any(array[777, 888]);
select count(*) from vectorqual where not metric3 === 777;
select count(*) from vectorqual where metric3 = 777 or metric3 === 777;

-- Custom operator that can be vectorized but doesn't have a negator.
create operator !!! (function = 'int4ne', rightarg = int4, leftarg = int4);
set timescaledb.debug_require_vector_qual to 'require';
select count(*) from vectorqual where metric3 !!! 777;
select count(*) from vectorqual where metric3 !!! any(array[777, 888]);
select count(*) from vectorqual where metric3 !!! 777 or metric3 !!! 888;
select count(*) from vectorqual where metric3 !!! 666 and (metric3 !!! 777 or metric3 !!! 888);
select count(*) from vectorqual where metric3 !!! 666 and (metric3 !!! 777 or metric3 !!! stable_abs(888));

set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where not metric3 !!! 777;
select count(*) from vectorqual where metric3 !!! 666 or (metric3 !!! 777 and not metric3 !!! 888);
select count(*) from vectorqual where metric3 !!! 666 or not (metric3 !!! 777 and not metric3 !!! 888);

set timescaledb.debug_require_vector_qual to 'allow';
select count(*) from vectorqual where metric3 !!! 777 or not metric3 !!! 888;
select count(*) from vectorqual where metric3 !!! 777 and not metric3 !!! 888;
select count(*) from vectorqual where not(metric3 !!! 666 or not (metric3 !!! 777 and not metric3 !!! 888));

-- These operators don't have a commutator.
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where 777 === metric3;
select count(*) from vectorqual where 777 !!! metric3;


-- NullTest is vectorized.
set timescaledb.debug_require_vector_qual to 'require';
select count(*) from vectorqual where metric4 is null;
select count(*) from vectorqual where metric4 is not null;
select count(*) from vectorqual where metric3 = 777 or metric4 is not null;
select count(*) from vectorqual where metric3 = stable_abs(777) or metric4 is null;


-- Can't vectorize conditions on system columns. Have to check this on a single
-- chunk, otherwise the whole-row var will be masked by ConvertRowType.
select show_chunks('vectorqual') chunk1 limit 1 \gset
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from :chunk1 t where t is null;
select count(*) from :chunk1 t where t.* is null;
select count(*) from :chunk1 t where tableoid is null;


-- Scalar array operators are vectorized if the operator is vectorizable.
set timescaledb.debug_require_vector_qual to 'require';
select count(*) from vectorqual where metric3 = any(array[777, 888]); /* default value */
select count(*) from vectorqual where metric4 = any(array[44, 55]) /* default null */;
select count(*) from vectorqual where metric2 > any(array[-1, -2, -3]) /* any */;
select count(*) from vectorqual where metric2 > all(array[-1, -2, -3]) /* all */;

-- Also have to support null array elements, because they are impossible to
-- prevent in stable expressions.
set timescaledb.debug_require_vector_qual to 'require';
select count(*) from vectorqual where metric2 = any(array[null::int]) /* any with null element */;
select count(*) from vectorqual where metric2 = any(array[22, null]) /* any with null element */;
select count(*) from vectorqual where metric2 = any(array[null, 32]) /* any with null element */;
select count(*) from vectorqual where metric2 = any(array[22, null, 32]) /* any with null element */;
select count(*) from vectorqual where metric2 = all(array[null::int]) /* all with null element */;
select count(*) from vectorqual where metric2 = all(array[22, null]) /* all with null element */;
select count(*) from vectorqual where metric2 = all(array[null, 32]) /* all with null element */;
select count(*) from vectorqual where metric2 = all(array[22, null, 32]) /* all with null element */;
select count(*) from vectorqual where metric2 = all(array[]::int[]);
select count(*) from vectorqual where metric2 = any(array[]::int[]);
select count(*) from vectorqual where metric2 = all(null::int[]);
select count(*) from vectorqual where metric2 = any(null::int[]);

-- Check early exit.
reset timescaledb.debug_require_vector_qual;
create table singlebatch(like vectorqual);
select create_hypertable('singlebatch', 'ts');
alter table singlebatch set (timescaledb.compress);
insert into singlebatch select '2022-02-02 02:02:02', metric2, device, metric3, metric4, tag from vectorqual;
select count(compress_chunk(x, true)) from show_chunks('singlebatch') x;

set timescaledb.debug_require_vector_qual to 'require';
-- Uncomment to generate the test reference w/o the vector optimizations.
-- set timescaledb.enable_bulk_decompression to off;
-- set timescaledb.debug_require_vector_qual to 'forbid';

select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 22]);
select count(*) from singlebatch where metric2 = any(array[0, 22, 0, 0, 0]);
select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 0]);
select count(*) from singlebatch where metric2 != any(array[0, 0, 0, 0, 0]);
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 0]);
select count(*) from singlebatch where metric2 <= all(array[12, 0, 12, 12, 12]);
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 12]);

select count(*) from singlebatch where metric3 = 777 and metric2 = any(array[0, 0, 0, 0, 22]);
select count(*) from singlebatch where metric3 = 777 and metric2 = any(array[0, 22, 0, 0, 0]);
select count(*) from singlebatch where metric3 = 777 and metric2 = any(array[0, 0, 0, 0, 0]);
select count(*) from singlebatch where metric3 = 777 and metric2 != any(array[0, 0, 0, 0, 0]);
select count(*) from singlebatch where metric3 = 777 and metric2 <= all(array[12, 12, 12, 12, 0]);
select count(*) from singlebatch where metric3 = 777 and metric2 <= all(array[12, 0, 12, 12, 12]);
select count(*) from singlebatch where metric3 = 777 and metric2 <= all(array[12, 12, 12, 12, 12]);

select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 22]) and metric3 = 777;
select count(*) from singlebatch where metric2 = any(array[0, 22, 0, 0, 0]) and metric3 = 777;
select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 0]) and metric3 = 777;
select count(*) from singlebatch where metric2 != any(array[0, 0, 0, 0, 0]) and metric3 = 777;
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 0]) and metric3 = 777;
select count(*) from singlebatch where metric2 <= all(array[12, 0, 12, 12, 12]) and metric3 = 777;
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 12]) and metric3 = 777;

select count(*) from singlebatch where metric3 != 777 and metric2 = any(array[0, 0, 0, 0, 22]);
select count(*) from singlebatch where metric3 != 777 and metric2 = any(array[0, 22, 0, 0, 0]);
select count(*) from singlebatch where metric3 != 777 and metric2 = any(array[0, 0, 0, 0, 0]);
select count(*) from singlebatch where metric3 != 777 and metric2 != any(array[0, 0, 0, 0, 0]);
select count(*) from singlebatch where metric3 != 777 and metric2 <= all(array[12, 12, 12, 12, 0]);
select count(*) from singlebatch where metric3 != 777 and metric2 <= all(array[12, 0, 12, 12, 12]);
select count(*) from singlebatch where metric3 != 777 and metric2 <= all(array[12, 12, 12, 12, 12]);

select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 22]) and metric3 != 777;
select count(*) from singlebatch where metric2 = any(array[0, 22, 0, 0, 0]) and metric3 != 777;
select count(*) from singlebatch where metric2 = any(array[0, 0, 0, 0, 0]) and metric3 != 777;
select count(*) from singlebatch where metric2 != any(array[0, 0, 0, 0, 0]) and metric3 != 777;
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 0]) and metric3 != 777;
select count(*) from singlebatch where metric2 <= all(array[12, 0, 12, 12, 12]) and metric3 != 777;
select count(*) from singlebatch where metric2 <= all(array[12, 12, 12, 12, 12]) and metric3 != 777;


-- Also check early exit for AND/OR. Top-level clause must be OR, because top-level
-- AND is flattened into a list.
select count(*) from singlebatch where (metric2 < 20 and metric2 < 30) or metric3 = 777;
select count(*) from singlebatch where (metric2 < 30 and metric2 < 20) or metric3 = 777;
select count(*) from singlebatch where metric3 = 777 or (metric2 < 20 and metric2 < 30);
select count(*) from singlebatch where metric3 = 777 or (metric2 < 30 and metric2 < 20);

select count(*) from vectorqual where (metric2 < 20 and metric2 < 30) or metric3 = 777;
select count(*) from vectorqual where (metric2 < 30 and metric2 < 20) or metric3 = 777;
select count(*) from vectorqual where metric3 = 777 or (metric2 < 20 and metric2 < 30);
select count(*) from vectorqual where metric3 = 777 or (metric2 < 30 and metric2 < 20);

select count(*) from singlebatch where metric2 < 20 or metric3 < 50 or metric3 > 50;
select count(*) from singlebatch where metric2 < 20 or metric3 > 50 or metric3 < 50;
select count(*) from singlebatch where metric3 < 50 or metric2 < 20 or metric3 > 50;
select count(*) from singlebatch where metric3 > 50 or metric3 < 50 or metric2 < 20;

select count(*) from vectorqual where metric2 < 20 or metric3 < 50 or metric3 > 50;
select count(*) from vectorqual where metric2 < 20 or metric3 > 50 or metric3 < 50;
select count(*) from vectorqual where metric3 < 50 or metric2 < 20 or metric3 > 50;
select count(*) from vectorqual where metric3 > 50 or metric3 < 50 or metric2 < 20;

select count(*) from singlebatch where metric2 = 12 or metric3 = 888;
select count(*) from singlebatch where metric2 = 22 or metric3 = 888;
select count(*) from singlebatch where metric2 = 32 or metric3 = 888;
select count(*) from singlebatch where metric2 = 42 or metric3 = 888;
select count(*) from singlebatch where metric2 = 52 or metric3 = 888;

select count(*) from vectorqual where metric2 = 12 or metric3 = 888;
select count(*) from vectorqual where metric2 = 22 or metric3 = 888;
select count(*) from vectorqual where metric2 = 32 or metric3 = 888;
select count(*) from vectorqual where metric2 = 42 or metric3 = 888;
select count(*) from vectorqual where metric2 = 52 or metric3 = 888;

select count(*) from singlebatch where ts > '2024-01-01' or (metric3 = 777 and metric2 = 12);
select count(*) from singlebatch where ts > '2024-01-01' or (metric3 = 777 and metric2 = 666);
select count(*) from singlebatch where ts > '2024-01-01' or (metric3 = 888 and metric2 = 12);
select count(*) from singlebatch where ts > '2024-01-01' or (metric3 = 888 and metric2 = 666);

select count(*) from vectorqual where ts > '2024-01-01' or (metric3 = 777 and metric2 = 12);
select count(*) from vectorqual where ts > '2024-01-01' or (metric3 = 777 and metric2 = 666);
select count(*) from vectorqual where ts > '2024-01-01' or (metric3 = 888 and metric2 = 12);
select count(*) from vectorqual where ts > '2024-01-01' or (metric3 = 888 and metric2 = 666);


-- On versions >= 14, the Postgres planner chooses to build a hash table for
-- large arrays. We currently don't vectorize in this case.
select 1 from set_config('timescaledb.debug_require_vector_qual',
    case when current_setting('server_version_num')::int >= 140000 then 'forbid' else 'require' end,
    false);

select count(*) from singlebatch where metric2 = any(array[
 0,  1,  2,  3,  4,  5,  6,  7,  8,  9,
10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
90, 91, 92, 93, 94, 95, 96, 97, 98, 99
]::int8[]);

reset timescaledb.enable_bulk_decompression;
reset timescaledb.debug_require_vector_qual;


-- Comparison with other column not vectorized.
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where metric3 = metric4;
select count(*) from vectorqual where metric3 = any(array[metric4]);


-- Vectorized filters also work if we have only stable functions on the right
-- side that can be evaluated to a constant at run time.
set timescaledb.debug_require_vector_qual to 'require';
select count(*) from vectorqual where ts > '2021-01-01 00:00:00'::timestamptz::timestamp;
select count(*) from vectorqual where ts > '2021-01-01 00:00:00'::timestamp - interval '1 day';
-- Expression that evaluates to Null.
select count(*) from vectorqual where ts > case when '2021-01-01'::timestamp < '2022-01-01'::timestamptz then null else '2021-01-01 00:00:00'::timestamp end;
select count(*) from vectorqual where ts < LOCALTIMESTAMP + '3 years'::interval;

-- These filters are not vectorizable as written, because the 'timestamp > timestamptz'
-- operator is stable, not immutable. We will try to cast the constant to the
-- same type in this case.
set timescaledb.debug_require_vector_qual to 'require';
select count(*) from vectorqual where ts > '2021-01-01 00:00:00'::timestamptz;
select count(*) from vectorqual where ts < LOCALTIMESTAMP at time zone 'UTC' + '3 years'::interval;


-- Can't vectorize comparison with a volatile function.
set timescaledb.debug_require_vector_qual to 'forbid';
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
set timescaledb.debug_require_vector_qual to 'require';
select count(*) from vectorqual where metric3 === 4;
\set ON_ERROR_STOP 1


-- Date columns
create table date_table(ts date);
select create_hypertable('date_table', 'ts');
alter table date_table set (timescaledb.compress);
insert into date_table values ('2021-01-01'), ('2021-01-02'),
    ('2021-01-03');
select count(compress_chunk(x, true)) from show_chunks('date_table') x;

set timescaledb.debug_require_vector_qual to 'require';
select * from date_table where ts >  '2021-01-02';
select * from date_table where ts >= '2021-01-02';
select * from date_table where ts =  '2021-01-02';
select * from date_table where ts <= '2021-01-02';
select * from date_table where ts <  '2021-01-02';
select * from date_table where ts <  CURRENT_DATE;

-- Text columns.
create table text_table(ts int, d int);
select create_hypertable('text_table', 'ts');
alter table text_table set (timescaledb.compress, timescaledb.compress_segmentby = 'd');

insert into text_table select x, 0 /*, default */ from generate_series(1, 1000) x;
select count(compress_chunk(x, true)) from show_chunks('text_table') x;
alter table text_table add column a text default 'default';

insert into text_table select x, 1, '' from generate_series(1, 1000) x;
insert into text_table select x, 2, 'same' from generate_series(1, 1000) x;
insert into text_table select x, 3, 'different' || x from generate_series(1, 1000) x;
insert into text_table select x, 4, case when x % 2 = 0 then null else 'same-with-nulls' end from generate_series(1, 1000) x;
insert into text_table select x, 5, case when x % 2 = 0 then null else 'different-with-nulls' || x end from generate_series(1, 1000) x;
insert into text_table select x, 6, 'одинаковый' from generate_series(1, 1000) x;
insert into text_table select x, 7, '異なる' || x from generate_series(1, 1000) x;

-- Some text values with varying lengths in a single batch. They are all different
-- to prevent dictionary compression, because we want to test particular orders
-- here as well.
insert into text_table select x,       8, repeat(        x::text || 'a',         x) from generate_series(1, 100) x;
insert into text_table select x + 100, 8, repeat((101 - x)::text || 'b', (101 - x)) from generate_series(1, 100) x;
insert into text_table select x + 200, 8, repeat((101 - x)::text || 'c', (101 - x)) from generate_series(1, 100) x;
insert into text_table select x + 300, 8, repeat(        x::text || 'd',         x) from generate_series(1, 100) x;

-- Use uncompressed table as reference.
set timescaledb.debug_require_vector_qual to 'forbid';
select sum(length(a)) from text_table;
select count(distinct a) from text_table;

select count(compress_chunk(x, true)) from show_chunks('text_table') x;
select compress_chunk(x) from show_chunks('text_table') x;

-- Check result with decompression.
set timescaledb.enable_bulk_decompression to on;
set timescaledb.debug_require_vector_qual to 'forbid';

select sum(length(a)) from text_table;
select count(distinct a) from text_table;


-- Test vectorized predicates.
set timescaledb.debug_require_vector_qual to 'require';
-- -- Uncomment to generate the test reference w/o the vector optimizations.
-- set timescaledb.enable_bulk_decompression to off;
-- set timescaledb.debug_require_vector_qual to 'forbid';

select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = 'default';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = '';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = 'same';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a != 'same';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = 'одинаковый';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = 'same-with-nulls';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = 'different1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = '異なる1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = 'different-with-nulls1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = 'different1000';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = 'different-with-nulls999';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a in ('same', 'different500');
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a in ('same-with-nulls', 'different-with-nulls499');
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a in ('different500', 'default');
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a = 'different500' or a = 'default';

select count(*), min(ts), max(ts), min(d), max(d) from text_table where a is null;
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a is not null;

select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%same%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%одинаковый%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%одилаковый%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%одимаковый%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%異なる%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%異オる%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%異にる%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '異_る_';

select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%different1%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like '%different1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different%%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different%%1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different%\1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different%_';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different%__';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different%___';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different%_1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different%nulls_';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different1%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different\%';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different\1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different_%1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different_';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different_1';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'same_';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a not like '%different1%';

\set ON_ERROR_STOP 0
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different\';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a like 'different%\';
\set ON_ERROR_STOP 1


-- We don't vectorize comparison operators with text because they are probably
-- not very useful.
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a < 'same';
select count(*), min(ts), max(ts), min(d), max(d) from text_table where a > 'same';

reset timescaledb.debug_require_vector_qual;
reset timescaledb.enable_bulk_decompression;

-- Test the nonstandard Postgres NaN comparison that doesn't match the IEEE floats.
create table nans(t int, cfloat4 float4, cfloat8 float8);
select create_hypertable('nans', 't', chunk_time_interval => 1024 * 1024 * 1024);
alter table nans set (timescaledb.compress);
insert into nans select pow(2, n), x, x
    from unnest(array[null, 0, 1, 'nan', '-inf', '+inf']::float8[])
        with ordinality as x(x, n)
;
select count(compress_chunk(x)) from show_chunks('nans') x;

set timescaledb.enable_bulk_decompression to on;
set timescaledb.debug_require_vector_qual to 'require';

select format('select sum(t) from nans where %s %s %s::%s;',
    variable, op, value, type)
from
    unnest(array['cfloat4', 'cfloat8']) variable,
    unnest(array['=', '!=', '<', '<=', '>', '>=']) op,
    unnest(array['null', '0', '1', '''nan''', '''-inf''', '''+inf''']) value,
    unnest(array['float4', 'float8', 'numeric']) type
\gexec

reset timescaledb.debug_require_vector_qual;
reset timescaledb.enable_bulk_decompression;

-- Test predicates on UUID columns can be vectorized
-- if UUID compression is _disabled_. With the new changes
-- vectorized decompression should be supported, regardless of
-- whether the UUID compression is enabled or not.
set timescaledb.enable_uuid_compression = off;

-- Generate reference results with uncommenting the next two lines
-- set timescaledb.enable_bulk_decompression to off;
-- set timescaledb.debug_require_vector_qual to 'forbid';

create table uuid_table(ts int, u uuid);
select count(*) from create_hypertable('uuid_table', 'ts');
alter table uuid_table set (timescaledb.compress);
insert into uuid_table values
	(1, '0197a7a9-b48b-7c70-be05-e376afc66ee1'), (2, '0197a7a9-b48b-7c71-92cb-eb724822bb0f'), (3, '0197a7a9-b48b-7c72-bd57-49f981f064fd'), (4, '0197a7a9-b48b-7c73-b521-91172c2e770a'),
	(5, '0197a7a9-b48b-7c74-a2a4-dcdbce635d11'), (6, '0197a7a9-b48b-7c75-a810-8acf630e634f'), (7, '0197a7a9-b48b-7c76-b616-69e64a802b5c'), (8, '0197a7a9-b48b-7c77-b54f-c5f3d64d68d0'),
	(9, '0197a7a9-b48b-7c78-ab14-78b3dd81dbbc'), (10, '0197a7a9-b48b-7c79-92c7-7dde3bea6252'), (11, '0197a7a9-b48b-7c7a-8d9e-5afc3bf15234'), (12, '0197a7a9-b48b-7c7b-bc49-7150f16d8d63'),
	(13, '0197a7a9-b48b-7c7c-aa7a-60d47bf04ff8'), (14, '0197a7a9-b48b-7c7d-8cfe-9503ed9bb1c9'), (15, '0197a7a9-b48b-7c7e-9ebb-acf63f5b625e'), (16, '0197a7a9-b48b-7c7f-a0c1-ba4adf950a2a'),
    (17, '01941f29-7c00-706a-bea9-105dad841304'), (18, '01941f2a-665f-7722-b4b5-cf4e70e666d0'),
	(19, NULL::uuid), (20, NULL::uuid), (21, NULL::uuid);
select count(compress_chunk(x, true)) from show_chunks('uuid_table') x;

-- With the new changes bulk decompression and vectorized filtering
-- will be supported regardless of whether the UUID compression is
-- enabled or not.

set timescaledb.debug_require_vector_qual to 'require';
select * from uuid_table where u = '01941f29-7c00-706a-bea9-105dad841304'::uuid;
select * from uuid_table where u in (
	'01941f29-7c00-706a-bea9-105dad841304'::uuid,
	'0197a7a9-b48b-7c75-a810-8acf630e634f'::uuid)
order by 1, 2;
select * from uuid_table where u = any(array[
	'01941f29-7c00-706a-bea9-105dad841304'::uuid,
	'0197a7a9-b48b-7c75-a810-8acf630e634f'::uuid
]) order by 1, 2;
select * from uuid_table where u not in (
	'0197a7a9-b48b-7c70-be05-e376afc66ee1'::uuid,
	'0197a7a9-b48b-7c71-92cb-eb724822bb0f'::uuid)
order by 1, 2;
select * from uuid_table where u is not null order by 1, 2;

-- Modify the data to make sure dictionary compression is used as well.
update uuid_table set u = '0197a7a9-b48b-7c70-be05-e376afc66ee1' where ts in (1, 2, 3, 4);
update uuid_table set u = '0197a7a9-b48b-7c71-92cb-eb724822bb0f' where ts in (5, 6, 7, 8);
update uuid_table set u = '0197a7a9-b48b-7c72-bd57-49f981f064fd' where ts in (9, 10, 11, 12);
update uuid_table set u = '0197a7a9-b48b-7c73-b521-91172c2e770a' where ts in (13, 14, 15, 16);
select count(compress_chunk(x, true)) from show_chunks('uuid_table') x;

-- Generate reference results with uncommenting the next two lines
-- set timescaledb.enable_bulk_decompression to off;
-- set timescaledb.debug_require_vector_qual to 'forbid';

select * from uuid_table where u = '01941f29-7c00-706a-bea9-105dad841304'::uuid;
select * from uuid_table where u in (
	'01941f29-7c00-706a-bea9-105dad841304'::uuid,
	'0197a7a9-b48b-7c75-a810-8acf630e634f'::uuid)
order by 1, 2;
select * from uuid_table where u = any(array[
	'01941f29-7c00-706a-bea9-105dad841304'::uuid,
	'0197a7a9-b48b-7c75-a810-8acf630e634f'::uuid
]) order by 1, 2;
select * from uuid_table where u not in (
	'0197a7a9-b48b-7c70-be05-e376afc66ee1'::uuid,
	'0197a7a9-b48b-7c71-92cb-eb724822bb0f'::uuid)
order by 1, 2;
select * from uuid_table where u is not null order by 1, 2;

reset timescaledb.enable_bulk_decompression;
reset timescaledb.debug_require_vector_qual;

-- Test predicates on UUID columns can be vectorized
-- if UUID compression is _enabled_
set timescaledb.enable_uuid_compression = on;

-- Generate reference results with uncommenting the next two lines
-- set timescaledb.enable_bulk_decompression to off;
-- set timescaledb.debug_require_vector_qual to 'forbid';

delete from uuid_table;
insert into uuid_table values
	(1, '0197a7a9-b48b-7c70-be05-e376afc66ee1'), (2, '0197a7a9-b48b-7c71-92cb-eb724822bb0f'), (3, '0197a7a9-b48b-7c72-bd57-49f981f064fd'), (4, '0197a7a9-b48b-7c73-b521-91172c2e770a'),
	(5, '0197a7a9-b48b-7c74-a2a4-dcdbce635d11'), (6, '0197a7a9-b48b-7c75-a810-8acf630e634f'), (7, '0197a7a9-b48b-7c76-b616-69e64a802b5c'), (8, '0197a7a9-b48b-7c77-b54f-c5f3d64d68d0'),
	(9, '0197a7a9-b48b-7c78-ab14-78b3dd81dbbc'), (10, '0197a7a9-b48b-7c79-92c7-7dde3bea6252'), (11, '0197a7a9-b48b-7c7a-8d9e-5afc3bf15234'), (12, '0197a7a9-b48b-7c7b-bc49-7150f16d8d63'),
	(13, '0197a7a9-b48b-7c7c-aa7a-60d47bf04ff8'), (14, '0197a7a9-b48b-7c7d-8cfe-9503ed9bb1c9'), (15, '0197a7a9-b48b-7c7e-9ebb-acf63f5b625e'), (16, '0197a7a9-b48b-7c7f-a0c1-ba4adf950a2a'),
    (17, '01941f29-7c00-706a-bea9-105dad841304'), (18, '01941f2a-665f-7722-b4b5-cf4e70e666d0'),
	(19, NULL::uuid), (20, NULL::uuid), (21, NULL::uuid);
select count(compress_chunk(x, true)) from show_chunks('uuid_table') x;

set timescaledb.debug_require_vector_qual to 'require';
select * from uuid_table where u = '01941f29-7c00-706a-bea9-105dad841304'::uuid;
select * from uuid_table where u in (
	'01941f29-7c00-706a-bea9-105dad841304'::uuid,
	'0197a7a9-b48b-7c75-a810-8acf630e634f'::uuid)
order by 1, 2;
select * from uuid_table where u = any(array[
	'01941f29-7c00-706a-bea9-105dad841304'::uuid,
	'0197a7a9-b48b-7c75-a810-8acf630e634f'::uuid
]) order by 1, 2;
select * from uuid_table where u not in (
	'0197a7a9-b48b-7c70-be05-e376afc66ee1'::uuid,
	'0197a7a9-b48b-7c71-92cb-eb724822bb0f'::uuid)
order by 1, 2;
select * from uuid_table where u is not null order by 1, 2;

reset timescaledb.enable_bulk_decompression;
reset timescaledb.debug_require_vector_qual;
reset timescaledb.enable_uuid_compression;

-- Test predicates on boolean columns can be vectorized
-- with the bool compression enabled.
set timescaledb.enable_bulk_decompression to on;
set timescaledb.enable_bool_compression = on;

create table bool_table(ts int, b bool);
select count(*) from create_hypertable('bool_table', 'ts');
alter table bool_table set (timescaledb.compress);
insert into bool_table values (100, true), (101, false), (102, null);

select count(compress_chunk(x, true)) from show_chunks('bool_table') x;
set timescaledb.debug_require_vector_qual to 'require';

-- BooleanTest expressions
select * from bool_table where b is true order by 1;
select * from bool_table where b is false order by 1;
select * from bool_table where b is not true order by 1;
select * from bool_table where b is not false order by 1;
select * from bool_table where b is unknown order by 1;
select * from bool_table where b is not unknown order by 1;

-- Combinations of nulls and bool exprs
select * from bool_table where b = true order by 1;
select * from bool_table where b = true or b = false order by 1;
select * from bool_table where b = true and b = false order by 1;
select * from bool_table where b = true or b is null order by 1;
select * from bool_table where b = true or b is not null order by 1;
select * from bool_table where b = true and b is null order by 1;
select * from bool_table where b = true and b is not null order by 1;
select * from bool_table where b = false order by 1;
select * from bool_table where b = false or b is null order by 1;
select * from bool_table where b = false or b is not null order by 1;
select * from bool_table where b = false and b is null order by 1;
select * from bool_table where b = false and b is not null order by 1;
select * from bool_table where b is null order by 1;
select * from bool_table where b is null or b is not null order by 1;
select * from bool_table where b is null and b is not null order by 1;
select * from bool_table where b is not null order by 1;

delete from bool_table where b is null;
select count(compress_chunk(x, true)) from show_chunks('bool_table') x;

select * from bool_table where b is true order by 1;
select * from bool_table where b is false order by 1;
select * from bool_table where b is not true order by 1;
select * from bool_table where b is not false order by 1;
select * from bool_table where b is unknown order by 1;
select * from bool_table where b is not unknown order by 1;
select * from bool_table where b = true order by 1;
select * from bool_table where b = true or b = false order by 1;
select * from bool_table where b = true and b = false order by 1;
select * from bool_table where b = true or b is null order by 1;
select * from bool_table where b = true or b is not null order by 1;
select * from bool_table where b = true and b is null order by 1;
select * from bool_table where b = true and b is not null order by 1;
select * from bool_table where b = false order by 1;
select * from bool_table where b = false or b is null order by 1;
select * from bool_table where b = false or b is not null order by 1;
select * from bool_table where b = false and b is null order by 1;
select * from bool_table where b = false and b is not null order by 1;
select * from bool_table where b is null order by 1;
select * from bool_table where b is null or b is not null order by 1;
select * from bool_table where b is null and b is not null order by 1;
select * from bool_table where b is not null order by 1;

reset timescaledb.debug_require_vector_qual;
reset timescaledb.enable_bulk_decompression;
reset timescaledb.enable_bool_compression;

-- Check that the bool compression changes didn't mess up the
-- array compression support for the bools.
delete from bool_table;
set timescaledb.enable_bool_compression = off;
insert into bool_table values (100, true), (101, false), (102, null);
select count(compress_chunk(x, true)) from show_chunks('bool_table') x;

select * from bool_table where b is true order by 1;
select * from bool_table where b is false order by 1;
select * from bool_table where b is not true order by 1;
select * from bool_table where b is not false order by 1;
select * from bool_table where b is unknown order by 1;
select * from bool_table where b is not unknown order by 1;
select * from bool_table where b = true order by 1;
select * from bool_table where b = true or b = false order by 1;
select * from bool_table where b = true and b = false order by 1;
select * from bool_table where b = true or b is null order by 1;
select * from bool_table where b = true or b is not null order by 1;
select * from bool_table where b = true and b is null order by 1;
select * from bool_table where b = true and b is not null order by 1;
select * from bool_table where b = false order by 1;
select * from bool_table where b = false or b is null order by 1;
select * from bool_table where b = false or b is not null order by 1;
select * from bool_table where b = false and b is null order by 1;
select * from bool_table where b = false and b is not null order by 1;
select * from bool_table where b is null order by 1;
select * from bool_table where b is null or b is not null order by 1;
select * from bool_table where b is null and b is not null order by 1;
select * from bool_table where b is not null order by 1;

delete from bool_table where b is null;
select count(compress_chunk(x, true)) from show_chunks('bool_table') x;

select * from bool_table where b is true order by 1;
select * from bool_table where b is false order by 1;
select * from bool_table where b is not true order by 1;
select * from bool_table where b is not false order by 1;
select * from bool_table where b is unknown order by 1;
select * from bool_table where b is not unknown order by 1;
select * from bool_table where b = true order by 1;
select * from bool_table where b = true or b = false order by 1;
select * from bool_table where b = true and b = false order by 1;
select * from bool_table where b = true or b is null order by 1;
select * from bool_table where b = true or b is not null order by 1;
select * from bool_table where b = true and b is null order by 1;
select * from bool_table where b = true and b is not null order by 1;
select * from bool_table where b = false order by 1;
select * from bool_table where b = false or b is null order by 1;
select * from bool_table where b = false or b is not null order by 1;
select * from bool_table where b = false and b is null order by 1;
select * from bool_table where b = false and b is not null order by 1;
select * from bool_table where b is null order by 1;
select * from bool_table where b is null or b is not null order by 1;
select * from bool_table where b is null and b is not null order by 1;
select * from bool_table where b is not null order by 1;

-- At this point the bool data is generated as 'bool compression disabed'
-- meaning, it is compressed with array compression. I try to confuse the
-- executor by creating a vectorized plan and still hoping to get the right
-- results.
set timescaledb.debug_require_vector_qual to 'require';
set timescaledb.enable_bool_compression = on;
select * from bool_table where b is true order by 1;
select * from bool_table where b is false order by 1;
select * from bool_table where b is not true order by 1;
select * from bool_table where b is not false order by 1;
select * from bool_table where b is unknown order by 1;
select * from bool_table where b is not unknown order by 1;
select * from bool_table where b = true order by 1;
select * from bool_table where b = true or b = false order by 1;
select * from bool_table where b = true and b = false order by 1;
select * from bool_table where b = true or b is null order by 1;
select * from bool_table where b = true or b is not null order by 1;
select * from bool_table where b = true and b is null order by 1;
select * from bool_table where b = true and b is not null order by 1;
select * from bool_table where b = false order by 1;
select * from bool_table where b = false or b is null order by 1;
select * from bool_table where b = false or b is not null order by 1;
select * from bool_table where b = false and b is null order by 1;
select * from bool_table where b = false and b is not null order by 1;
select * from bool_table where b is null order by 1;
select * from bool_table where b is null or b is not null order by 1;
select * from bool_table where b is null and b is not null order by 1;
select * from bool_table where b is not null order by 1;

-- Verify that NULL compression with bools is vectorized
set timescaledb.debug_require_vector_qual to 'require';
update bool_table set b = NULL;
select count(compress_chunk(x, true)) from show_chunks('bool_table') x;
select * from bool_table where b is null order by 1;
select * from bool_table where b is not null order by 1;
select * from bool_table where b is null or b is not null order by 1;
select * from bool_table where b is null and b is not null order by 1;
select * from bool_table where b is not true order by 1;
select * from bool_table where b is not false order by 1;
select * from bool_table where b is unknown order by 1;
select * from bool_table where b is not unknown order by 1;

-- Verify that NULL compression with ints is vectorized
create table int_table(ts int, b int);
select count(*) from create_hypertable('int_table', 'ts');
alter table int_table set (timescaledb.compress);
insert into int_table values (100, NULL), (101, NULL), (102, NULL);

select count(compress_chunk(x, true)) from show_chunks('int_table') x;
select * from int_table where b is null order by 1;
select * from int_table where b is not null order by 1;
select * from int_table where b is null or b is not null order by 1;
select * from int_table where b is null and b is not null order by 1;

-- check the compression algorithm for the compressed chunks
set timescaledb.enable_transparent_decompression='off';
DO $$
DECLARE
	comp_regclass REGCLASS;
	rec RECORD;
BEGIN
	FOR comp_regclass IN
		SELECT
			format('%I.%I', comp.schema_name, comp.table_name)::regclass as comp_regclass
		FROM
			_timescaledb_catalog.chunk uncomp,
			_timescaledb_catalog.chunk comp,
			(SELECT show_chunks('bool_table') as c UNION SELECT show_chunks('int_table') as c) as x
		WHERE
			uncomp.dropped IS FALSE AND uncomp.compressed_chunk_id IS NOT NULL AND
			comp.id = uncomp.compressed_chunk_id AND
			x.c = format('%I.%I', uncomp.schema_name, uncomp.table_name)::regclass
	LOOP
		FOR rec IN
			EXECUTE format('SELECT b, _timescaledb_functions.compressed_data_info(b) FROM %s', comp_regclass)
		LOOP
			RAISE NOTICE 'Compressed info results: %', rec;
		END LOOP;

		FOR rec IN
			EXECUTE format('SELECT b, _timescaledb_functions.compressed_data_has_nulls(b) FROM %s', comp_regclass)
		LOOP
			RAISE NOTICE 'Has nulls results: %', rec;
		END LOOP;

	END LOOP;
END;
$$;

reset timescaledb.enable_transparent_decompression;
reset timescaledb.debug_require_vector_qual;
reset timescaledb.enable_bool_compression;

