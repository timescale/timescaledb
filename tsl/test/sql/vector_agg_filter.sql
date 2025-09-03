-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- helper function: float -> pseudorandom float [-0.5..0.5]
CREATE OR REPLACE FUNCTION mix(x anyelement) RETURNS float8 AS $$
    SELECT hashfloat8(x::float8) / pow(2, 32)
$$ LANGUAGE SQL;
-- non-vectorizable equality operator
create operator === (function = 'int4eq', rightarg = int4, leftarg = int4);
-- an abs() function that is stable not immutable
create function stable_abs(x int4) returns int4 as 'int4abs' language internal stable;

\set CHUNKS 2::int
\set CHUNK_ROWS 100000::int
\set GROUPING_CARDINALITY 10::int

create table aggfilter(t int, s int,
    cint2 int2, dropped int4, cint4 int4);
select create_hypertable('aggfilter', 's', chunk_time_interval => :GROUPING_CARDINALITY / :CHUNKS);

create view source as
select s * 10000 + t as t,
    s,
    case when t % 1051 = 0 then null
        else (mix(s + t * 1019) * 32767)::int2 end as cint2,
    1 as dropped,
    (mix(s + t * 1021) * 32767)::int4 as cint4
from
    generate_series(1::int, :CHUNK_ROWS * :CHUNKS / :GROUPING_CARDINALITY) t,
    generate_series(0::int, :GROUPING_CARDINALITY - 1::int) s(s)
;

insert into aggfilter select * from source where s = 1;

alter table aggfilter set (timescaledb.compress, timescaledb.compress_orderby = 't',
    timescaledb.compress_segmentby = 's');

select count(compress_chunk(x)) from show_chunks('aggfilter') x;

alter table aggfilter add column ss int default 11;
alter table aggfilter drop column dropped;

insert into aggfilter
select t, s, cint2, cint4,
    case
        -- null in entire batch
        when s = 2 then null
        -- null for some rows
        when s = 3 and t % 1053 = 0 then null
        -- for some rows same as default
        when s = 4 and t % 1057 = 0 then 11
        -- not null for entire batch
        else s
    end as ss
from source where s != 1
;

-- print a few reference values before compression
select count(ss) from aggfilter;
select count(ss) filter (where cint2 < 0) from aggfilter;
select count(ss) filter (where cint4 > 0) from aggfilter;
select count(ss) filter (where s != 5) from aggfilter;
select s, count(ss) from aggfilter group by s having s=2 order by count(ss), s limit 10;
select s, count(ss) filter (where cint2 < 0) from aggfilter group by s having s=2 order by count(ss), s limit 10;
select s, count(ss) filter (where ss > 1000) from aggfilter group by s having s=2 order by count(ss), s limit 10;
select s, count(ss) filter (where cint4 > 0) from aggfilter group by s having s=2 order by count(ss), s limit 10;

select count(compress_chunk(x)) from show_chunks('aggfilter') x;
vacuum freeze analyze aggfilter;



set timescaledb.debug_require_vector_agg = 'require';
---- Uncomment to generate reference.
--set timescaledb.enable_vectorized_aggregation to off; set timescaledb.debug_require_vector_agg = 'allow';

set max_parallel_workers_per_gather = 0;

select
    format('%sselect %s%s(%s)%s from aggfilter%s%s%s;',
            explain,
            grouping || ', ',
            function, variable,
            ' filter (where ' || agg_filter || ')',
            ' where ' || condition,
            ' group by ' || grouping,
            format(' order by %s(%s), ', function, variable) || grouping || ' limit 10',
            function, variable)
from
    unnest(array[
        'explain (buffers off, costs off) ',
        null]) explain,
    unnest(array[
        's',
        'ss',
        'cint2',
        'cint4',
        '*']) variable,
    unnest(array[
        'min',
        'count']) function,
    unnest(array[
        null,
        'cint2 > 0',
        'cint2 is null']) with ordinality as condition(condition, n),
    unnest(array[
        null,
        's']) with ordinality as grouping(grouping, n),
    unnest(array[
        null,
        'cint2 < 0',
        'ss > 1000',
        'cint4 > 0',
        's != 5']) with ordinality as agg_filter(agg_filter, n)
where
    true
    and (explain is null /* or condition is null and grouping = 's' */)
    and (variable != '*' or function = 'count')
order by explain, condition.n, variable, function, grouping.n, agg_filter.n
\gexec

reset timescaledb.debug_require_vector_agg;

-- FILTER that is not vectorizable
set timescaledb.debug_require_vector_agg = 'forbid';
select count(*) filter (where cint2 === 0) from aggfilter;

-- FILTER with stable function
set timescaledb.debug_require_vector_agg = 'require';
select count(*) filter (where cint2 = stable_abs(0)) from aggfilter;

-- With hash grouping
select
    ss,
    count(*) filter (where s != 5),
    count(*) filter (where cint2 < 0)
from aggfilter
group by ss
order by 2, 3;

reset timescaledb.debug_require_vector_agg;


-- Grouping with a scalar UUID column (segmentby or default).
create table uuid_default(ts int, value int4)
    with (tsdb.hypertable, tsdb.partition_column = 'ts', tsdb.compress,
        tsdb.chunk_interval = 1000);

insert into uuid_default select generate_series(0, 999), 1;

select count(compress_chunk(x)) from show_chunks('uuid_default') x;

alter table uuid_default add column id uuid default '842ab294-923a-4f50-be7d-af6c51903a5f';

alter table uuid_default set (tsdb.compress_segmentby = 'id');

insert into uuid_default select generate_series(1000, 1999), 2, '5dd0565f-1ddf-4a6c-9e96-9b2b8c8c3993';

select count(compress_chunk(x)) from show_chunks('uuid_default') x;

set timescaledb.debug_require_vector_agg = 'allow';

select id, sum(value) from uuid_default group by id;

set timescaledb.debug_require_vector_agg = 'require';

select sum(value) filter (where id = '5dd0565f-1ddf-4a6c-9e96-9b2b8c8c3993') from uuid_default;

select sum(value) filter (where id = '842ab294-923a-4f50-be7d-af6c51903a5f') from uuid_default;

reset timescaledb.debug_require_vector_agg;

reset max_parallel_workers_per_gather;
