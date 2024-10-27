
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
select count(compress_chunk(x)) from show_chunks('aggfilter') x;
vacuum freeze analyze aggfilter;



set timescaledb.debug_require_vector_agg = 'require';
---- Uncomment to generate reference. Note that there are minor discrepancies
---- on float4 due to different numeric stability in our and PG implementations.
--set timescaledb.enable_vectorized_aggregation to off; set timescaledb.debug_require_vector_agg = 'allow';

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
        'explain (costs off) ',
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
        's',
        'ss']) with ordinality as grouping(grouping, n),
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
