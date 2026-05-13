-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Expressions in vectorized aggregation.

\c :TEST_DBNAME :ROLE_SUPERUSER

set timescaledb.enable_columnarindexscan = off;

\pset null $

create function always_null(x int4) returns int4 as $$ select null::int4 $$
language sql strict immutable parallel safe;

create or replace function throw_on_twelve(n integer)
returns integer language plpgsql strict immutable parallel safe as $$
begin
  if n = 12 then
    raise exception 'n = 12';
  end if;
  return n;
end;
$$;

create table aggexpr(ts int, i int, x text, b bool, v float4) with (tsdb.hypertable,
    tsdb.compress, tsdb.compress_orderby = 'ts', tsdb.compress_segmentby = 'i, x, b',
    tsdb.partition_column = 'ts', tsdb.chunk_interval = 10);

insert into aggexpr select 1, null, null, null, generate_series(1, 1499);

insert into aggexpr select 2, 2, '2', false, generate_series(1, 1493);

-- Batch divisible by 64 to test proper padding.
insert into aggexpr select 3, case when x % 3 = 1 then 3 else 0 end,
    case when x % 3 = 2 then '3' else '0' end,
    case when x % 3 = 0 then true else false end
from generate_series(1, 960) x;

select count(compress_chunk(x)) from show_chunks('aggexpr') x;

alter table aggexpr set (tsdb.compress_segmentby = '');

insert into aggexpr select 11, null, null, null, generate_series(1, 1489);

insert into aggexpr select 12, 12, '12', false, generate_series(1, 1487);

insert into aggexpr select 13, case when x % 2 = 0 then 13 else null end,
    case when x % 2 = 1 then '13' else null end, x % 3 = 0, x from generate_series(1, 1483) x;

insert into aggexpr select 14, case when x % 2 = 0 then 14 else null end,
    case when x % 2 = 0 then '14' || x::text else null end, x % 3 = 1, x from generate_series(1, 1481) x;

select count(compress_chunk(x)) from show_chunks('aggexpr') x;

vacuum full analyze aggexpr;

-- The batch sorted merge has very close costs for one query, and prevents
-- vectorized aggregation, so we have to get it out of the way.
set timescaledb.enable_decompression_sorted_merge to off;


-- Some functions we are not able to vectorize at the moment.
set timescaledb.debug_require_vector_agg = 'forbid';

-- Volatile expression
select sum((b and random() < 0.0)::int) from aggexpr;

-- Non-strict expression
select sum(length(format('%s', x))) from aggexpr;

-- No columnar representation for the expression (numeric)
select sum(factorial(i % 2)) from aggexpr;

reset timescaledb.debug_require_vector_agg;


-- Test some functions that are vectorizable.
set timescaledb.debug_require_vector_agg = 'require';
-- /* Uncomment to generate reference. */ set timescaledb.debug_require_vector_agg = 'forbid'; set timescaledb.enable_vectorized_aggregation to off;


select always_null(i) from aggexpr group by 1;

select
    format('select %s%s from aggexpr%s%s%s;',
            grouping || ', ',
            function,
            ' where ' || condition,
            ' group by ' || grouping,
            format(' order by %s, ', function) || grouping || ' limit 10')
from
    unnest(array[
        'count(*)'
        , 'count(i)'
        , 'count(x)'
        , 'count(b)'
        , 'sum((i = 12)::int)'
        , 'sum(abs(v - 500))'
        ]) with ordinality as function(function, n),
    unnest(array[
        null
        , 'b'
        , 'not b'
        , 'length(x) = 1'
        , 'length(x) < 0'
        , 'i % 2 = 0'
        ]) with ordinality as condition(condition, n),
    unnest(array[
        null
        , 'length(x)'
        , 'lower(x)'
        , 'i % 2'
        , 'ts'
        , 'b'
        , 'v - 501 > 0'
        ]) with ordinality as grouping(grouping, n)
order by grouping.n, condition.n, function.n
\gexec

reset timescaledb.debug_require_vector_agg;
reset timescaledb.enable_vectorized_aggregation;


-- Common subexpression elimination: same expression in multiple aggregates.
set timescaledb.debug_require_vector_agg = 'require';
-- /* Uncomment to generate reference. */ set timescaledb.debug_require_vector_agg = 'forbid'; set timescaledb.enable_vectorized_aggregation to off;

-- Same expression abs(v - 500) used in two different aggregates.
select sum(abs(v - 500)), count(abs(v - 500)) from aggexpr;

-- Same expression in aggregates with grouping.
select i % 2, sum(abs(v - 500)), count(abs(v - 500))
    from aggexpr group by i % 2 order by 1;

-- Grouping column expression also appears as aggregate argument.
select length(x), count(length(x)), sum(length(x))
    from aggexpr group by length(x) order by 1;

-- Nested common subexpressions: i % 2 appears in both grouping and aggregate.
select i % 2, sum((i % 2)::float4) from aggexpr group by i % 2 order by 1;

-- Multiple aggregates with the same complex expression on different functions.
select sum((i = 12)::int), count((i = 12)::int) from aggexpr;

-- Same expression with WHERE clause.
select sum(abs(v - 500)), count(abs(v - 500))
    from aggexpr where b order by 1;

-- Aggregate FILTER clause with shared expression: CSE is skipped for
-- FILTER aggregates, but other aggregates still share.
select sum(abs(v - 500)) filter (where b),
       count(abs(v - 500)),
       sum(abs(v - 500))
    from aggexpr;

-- Shared text-returning subexpression: lower(x) cached as DT_ArrowText,
-- reused by length() in the second aggregate.
select count(lower(x)), sum(length(lower(x))) from aggexpr;

-- Text shared expression with hash grouping: lower(x) shared between
-- grouping column and aggregate arguments.
select lower(x), count(lower(x)), sum(length(lower(x)))
    from aggexpr group by lower(x) order by 1 limit 10;

-- Hash grouping with WHERE clause and CSE.
select i % 2, sum(abs(v - 500)), count(abs(v - 500))
    from aggexpr where b group by i % 2 order by 1;

-- Selective WHERE: only partial rows pass, cached result has many invalid rows.
select sum(abs(v - 500)), count(abs(v - 500))
    from aggexpr where i > 13;

-- WHERE filtering rows that would cause division by zero. The cached
-- expression must be evaluated under the batch filter, not a wider one,
-- to avoid calling the strict integer division on filtered-out rows.
select sum(1 / i), count(1 / i) from aggexpr where i > 0;

-- Strict function always returning NULL: tests caching of a null result.
select sum(always_null(i)), count(always_null(i)) from aggexpr;

-- Multiple distinct shared expressions cached simultaneously.
select sum(abs(v - 500)), count(abs(v - 500)),
       sum(length(x)), count(length(x)) from aggexpr;

-- Shared subtree at depth 2: v - 500 is shared between abs(v - 500) (in sum
-- and count) and (v - 500 > 0)::int (in sum), exercising a cache hit inside
-- a non-cached outer expression.
select sum(abs(v - 500)), count(abs(v - 500)),
       sum((v - 500 > 0)::int) from aggexpr;

-- Nullable column across batches: some segmentby batches have i as all-null
-- scalar, non-segmentby batches have mixed null/non-null arrow.
select sum(i % 2 + 1), count(i % 2 + 1) from aggexpr;

-- Multiple shared expressions with hash grouping.
select i % 2, sum(abs(v - 500)), count(abs(v - 500)),
       sum(length(x)), count(length(x))
    from aggexpr group by i % 2 order by 1;

-- FILTER clause with hash grouping: FILTER aggregate excluded from CSE,
-- other aggregates still share.
select i % 2,
       sum(abs(v - 500)) filter (where b),
       count(abs(v - 500)),
       sum(abs(v - 500))
    from aggexpr group by i % 2 order by 1;

-- Two shared sibling subexpressions: abs(v - 500) and length(x) both appear
-- in both aggregates. Tests that the refcount walker continues to sibling
-- nodes after finding a shared subtree.
select sum(abs(v - 500) + length(x)), sum(abs(v - 500) - length(x)) from aggexpr;

-- Three aggregates sharing the same expression (refcount = 3). The refcount
-- walker must not over-count children: it visits abs(v - 500) three times,
-- recurses into v - 500 only on the first visit, and skips children on
-- subsequent visits. Children (v - 500) stay at refcount 1 and are not cached.
select sum(abs(v - 500)), count(abs(v - 500)), min(abs(v - 500)) from aggexpr;

-- Cached boolean subexpression consumed as function argument. The comparison
-- b = (i > 0) produces a boolean arrow (DT_ArrowBits) that is cached, then
-- the int4 cast consumes it via compressed_columns_to_postgres_data.
select sum((b = (i > 0))::int), count(b = (i > 0)) from aggexpr;

reset timescaledb.debug_require_vector_agg;
reset timescaledb.enable_vectorized_aggregation;

-- Some CASE statements are vectorized.
set timescaledb.debug_require_vector_agg = 'require';
-- /* Uncomment to generate reference. */ set timescaledb.debug_require_vector_agg = 'forbid'; set timescaledb.enable_vectorized_aggregation to off;

select sum(case when i > 10 then i else -i end) from aggexpr group by b order by 1;

select count(case when i > 10 then i end) from aggexpr group by v - 501 > 0 order by 1;

select sum(case when i > 10 then (case when i > 12 then length(x) else -length(x) end) end) from aggexpr group by v - 502 > 0;

select avg(case when v > 500 then v - 500 else 500 - v end) from aggexpr group by x order by 1 limit 10;

select count(*), case when v > 503 then x else 'something-else' end from aggexpr group by 2 order by 1, 2 limit 10;

-- The short circuit semantics for CASE is not implemented at the moment.
\set ON_ERROR_STOP 0
select count(*), case when i = 12 then 12 else throw_on_twelve(i) end from aggexpr group by 2 order by 1, 2 limit 10;
\set ON_ERROR_STOP 1

reset timescaledb.debug_require_vector_agg;

-- This form is not vectorized at the moment.
select count(*), case i when 12 then 1212 else i end from aggexpr group by 2 order by 1, 2 limit 10;

select sum(case i when ts then 1 else 0 end) from aggexpr group by b order by 1;

-- Non-vectorizable branches.
select count(*), case when i = 12 then 1212 else i::numeric end from aggexpr group by 2 order by 1, 2 limit 10;

select count(*), case when i::numeric = 12::numeric then 1212 else i end from aggexpr group by 2 order by 1, 2 limit 10;

-- Vectorizable WHEN branches but non-vectorizable ELSE (volatile function).
select sum(case when i > 10 then i else (random() * 0)::int end) from aggexpr group by b order by 1;


reset timescaledb.enable_columnarindexscan;
reset timescaledb.enable_decompression_sorted_merge;
