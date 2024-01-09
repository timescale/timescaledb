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
alter table vectorqual add column tag name;
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


-- ORed constrainst on multiple columns.
set timescaledb.debug_require_vector_qual to 'only';
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

-- It also doesn't have a commutator.
select count(*) from vectorqual where 777 === metric3;


-- NullTest is vectorized.
set timescaledb.debug_require_vector_qual to 'only';
select count(*) from vectorqual where metric4 is null;
select count(*) from vectorqual where metric4 is not null;


-- Can't vectorize conditions on system columns. Have to check this on a single
-- chunk, otherwise the whole-row var will be masked by ConvertRowType.
select show_chunks('vectorqual') chunk1 limit 1 \gset
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from :chunk1 t where t is null;
select count(*) from :chunk1 t where t.* is null;
select count(*) from :chunk1 t where tableoid is null;


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

reset timescaledb.enable_bulk_decompression;
reset timescaledb.debug_require_vector_qual;


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
select count(*) from vectorqual where ts < LOCALTIMESTAMP + '3 years'::interval;

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
select count(*) from vectorqual where metric3 === 4;
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
select * from date_table where ts <  CURRENT_DATE;


-- Vectorized comparison for text
create table t(ts int, d int);
select create_hypertable('t', 'ts');
alter table t set (timescaledb.compress, timescaledb.compress_segmentby = 'd');

insert into t select x, 0 /*, default */ from generate_series(1, 1000) x;
select count(compress_chunk(x, true)) from show_chunks('t') x;
alter table t add column a text default 'default';

insert into t select x, 1, '' from generate_series(1, 1000) x;
insert into t select x, 2, 'same' from generate_series(1, 1000) x;
insert into t select x, 3, 'different' || x from generate_series(1, 1000) x;
insert into t select x, 4, case when x % 2 = 0 then null else 'same-with-nulls' end from generate_series(1, 1000) x;
insert into t select x, 5, case when x % 2 = 0 then null else 'different-with-nulls' || x end from generate_series(1, 1000) x;

select count(compress_chunk(x, true)) from show_chunks('t') x;

set timescaledb.debug_require_vector_qual to 'only';
-- Uncomment to generate the test reference w/o the vector optimizations.
-- set timescaledb.enable_bulk_decompression to off;
-- set timescaledb.debug_require_vector_qual to 'forbid';

select count(*), min(ts), max(ts), min(d), max(d) from t where a = 'default';
select count(*), min(ts), max(ts), min(d), max(d) from t where a = '';
select count(*), min(ts), max(ts), min(d), max(d) from t where a = 'same';
select count(*), min(ts), max(ts), min(d), max(d) from t where a = 'same-with-nulls';
select count(*), min(ts), max(ts), min(d), max(d) from t where a = 'different1';
select count(*), min(ts), max(ts), min(d), max(d) from t where a = 'different-with-nulls1';
select count(*), min(ts), max(ts), min(d), max(d) from t where a = 'different1000';
select count(*), min(ts), max(ts), min(d), max(d) from t where a = 'different-with-nulls999';
select count(*), min(ts), max(ts), min(d), max(d) from t where a in ('same', 'different500');
select count(*), min(ts), max(ts), min(d), max(d) from t where a in ('same-with-nulls', 'different-with-nulls499');
select count(*), min(ts), max(ts), min(d), max(d) from t where a in ('different500', 'default');
select count(*), min(ts), max(ts), min(d), max(d) from t where a = 'different500' or a = 'default';

reset timescaledb.debug_require_vector_qual;
select count(distinct a) from t;

-- Null tests are not vectorized yet.
reset timescaledb.debug_require_vector_qual;
select count(*), min(ts), max(ts), min(d), max(d) from t where a is null;
select count(*), min(ts), max(ts), min(d), max(d) from t where a is not null;

reset timescaledb.debug_require_vector_qual;
reset timescaledb.enable_bulk_decompression;
