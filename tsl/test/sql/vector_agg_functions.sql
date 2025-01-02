-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
-- helper function: float -> pseudorandom float [-0.5..0.5]
CREATE OR REPLACE FUNCTION mix(x anyelement) RETURNS float8 AS $$
    SELECT hashfloat8(x::float8) / pow(2, 32)
$$ LANGUAGE SQL;

\set CHUNKS 2::int
\set CHUNK_ROWS 100000::int
\set GROUPING_CARDINALITY 10::int

create table aggfns(t int, s int,
    cint2 int2, cint4 int4, cint8 int8,
    cfloat4 float4,
    cts timestamp, ctstz timestamptz,
    cdate date);
select create_hypertable('aggfns', 's', chunk_time_interval => :GROUPING_CARDINALITY / :CHUNKS);

create view source as
select s * 10000 + t as t,
    s,
    case when t % 1051 = 0 then null
        else (mix(s + t * 1019) * 32767)::int2 end as cint2,
    (mix(s + t * 1021) * 32767)::int4 as cint4,
    (mix(s + t * 1031) * 32767)::int8 as cint8,
    case when s = 1 and t = 1061 then 'nan'::float4
        when s = 2 and t = 1061 then '+inf'::float4
        when s = 3 and t = 1061 then '-inf'::float4
        else (mix(s + t * 1033) * 100::int)::float4 end as cfloat4,
    '2021-01-01 01:01:01'::timestamp + interval '1 second' * (s * 10000) as cts,
    '2021-01-01 01:01:01'::timestamptz + interval '1 second' * (s * 10000) as ctstz,
    '2021-01-01'::date + interval '1 day' * (s * 10000) as cdate
from
    generate_series(1::int, :CHUNK_ROWS * :CHUNKS / :GROUPING_CARDINALITY) t,
    generate_series(0::int, :GROUPING_CARDINALITY - 1::int) s(s)
;

insert into aggfns select * from source where s = 1;

alter table aggfns set (timescaledb.compress, timescaledb.compress_orderby = 't',
    timescaledb.compress_segmentby = 's');

select count(compress_chunk(x)) from show_chunks('aggfns') x;

alter table aggfns add column ss int default 11;
alter table aggfns add column cfloat8 float8 default '13';
alter table aggfns add column x text default '11';

insert into aggfns
select *, ss::text as x from (
    select *
        , case
            -- null in entire batch
            when s = 2 then null
            -- null for some rows
            when s = 3 and t % 1053 = 0 then null
            -- for some rows same as default
            when s = 4 and t % 1057 = 0 then 11
            -- not null for entire batch
            else s
        end as ss
        , (mix(s + t * 1039) * 100)::float8 as cfloat8
    from source where s != 1
) t
;
select count(compress_chunk(x)) from show_chunks('aggfns') x;
vacuum freeze analyze aggfns;


create table edges(t int, s int, ss int, f1 int);
select create_hypertable('edges', 't', chunk_time_interval => 100000);
alter table edges set (timescaledb.compress, timescaledb.compress_segmentby='s');
insert into edges select
    s * 1000 + f1 as t,
    s,
    s,
    f1
from generate_series(0, 12) s,
    lateral generate_series(0, 60 + s + (s / 5::int) * 64 + (s / 10::int) * 2048) f1
;
insert into edges select 200000 t, 111 s, 111 ss, 1 f1;
select count(compress_chunk(x)) from show_chunks('edges') x;
vacuum freeze analyze edges;

-- We can't vectorize some aggregate functions on platforms withouth int128
-- support. Just relax the test requirements for them. I don't want to disable
-- this test in release builds, and don't want to have the guc in release builds,
-- so we'll assume we have int128 in all release builds.
select case when setting::bool then 'require' else 'allow' end guc_value
from pg_settings where name = 'timescaledb.debug_have_int128'
union all select 'require' guc_value
limit 1
\gset

set timescaledb.debug_require_vector_agg = :'guc_value';
---- Uncomment to generate reference. Note that there are minor discrepancies
---- on float4 due to different numeric stability in our and PG implementations.
--set timescaledb.enable_chunkwise_aggregation to off; set timescaledb.enable_vectorized_aggregation to off; set timescaledb.debug_require_vector_agg = 'forbid';

set max_parallel_workers_per_gather = 0;

select
    format('%sselect %s%s(%s) from aggfns%s%s%s;',
            explain,
            grouping || ', ',
            function, variable,
            ' where ' || condition,
            ' group by ' || grouping,
            format(' order by %s(%s), ', function, variable) || grouping || ' limit 10',
            function, variable)
from
    unnest(array[
        'explain (costs off) ',
        null]) explain,
    unnest(array[
        't',
        's',
        'ss',
        'cint2',
        'cint4',
        'cint8',
        'cfloat4',
        'cfloat8',
        'cts',
        'ctstz',
        'cdate',
        '*']) variable,
    unnest(array[
        'min',
        'max',
        'sum',
        'avg',
        'stddev',
        'count']) function,
    unnest(array[
        null,
        'cfloat8 > 0',
        'cfloat8 <= 0',
        'cfloat8 < 1000' /* vectorized qual is true for all rows */,
        'cfloat8 > 1000' /* vectorized qual is false for all rows */,
        'cint2 is null']) with ordinality as condition(condition, n),
    unnest(array[
        null,
        's',
        'ss']) with ordinality as grouping(grouping, n)
where
    true
    and (explain is null /* or condition is null and grouping = 's' */)
    and (variable != '*' or function = 'count')
    and (variable not in ('t', 'cts', 'ctstz', 'cdate') or function in ('min', 'max'))
    -- This is not vectorized yet
    and (variable != 'cint8' or function != 'stddev')
    and (function != 'count' or variable in ('cint2', 's', '*'))
    and (condition is distinct from 'cint2 is null' or variable = 'cint2')
order by explain, condition.n, variable, function, grouping.n
\gexec


-- Test multiple aggregate functions as well.
select count(*), count(cint2), min(cfloat4), cint2 from aggfns group by cint2
order by count(*) desc, cint2 limit 10
;

-- Test edge cases for various batch sizes and the filter matching around batch
-- end.
select count(*) from edges;
select s, count(*) from edges group by 1 order by 1;

select s, count(*), min(f1) from edges where f1 = 63 group by 1 order by 1;
select s, count(*), min(f1) from edges where f1 = 64 group by 1 order by 1;
select s, count(*), min(f1) from edges where f1 = 65 group by 1 order by 1;

select ss, count(*), min(f1) from edges where f1 = 63 group by 1 order by 1;
select ss, count(*), min(f1) from edges where f1 = 64 group by 1 order by 1;
select ss, count(*), min(f1) from edges where f1 = 65 group by 1 order by 1;

reset max_parallel_workers_per_gather;
