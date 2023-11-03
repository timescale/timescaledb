-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

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


-- NullTest is not vectorized.
set timescaledb.debug_require_vector_qual to 'forbid';
select count(*) from vectorqual where metric4 is null;
select count(*) from vectorqual where metric4 is not null;


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

-- Vectorized comparison for text
create table t(ts timestamp, a text);
select create_hypertable('t', 'ts');
alter table t set (timescaledb.compress);

insert into t select '2021-01-01 01:01:01'::timestamp + interval '1 second' * x, 'same'
from generate_series(1, 1000) x
;

insert into t select '2021-01-01 02:01:01'::timestamp + interval '1 second' * x, 'different' || x
from generate_series(1, 1000) x
;

select count(compress_chunk(x, true)) from show_chunks('t') x;

set timescaledb.debug_require_vector_qual to 'only';

select count(*), min(ts) from t where a = 'same';
select count(*), min(ts) from t where a = 'different1';
select count(*), min(ts) from t where a = 'different1000';
