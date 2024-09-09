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

create table aggfns(t int, s int, ss int,
    cint2 int2, cint4 int4, cint8 int8,
    cfloat4 float4, cfloat8 float8,
    cts timestamp, ctstz timestamptz,
    cdate date);
select create_hypertable('aggfns', 's', chunk_time_interval => :GROUPING_CARDINALITY / :CHUNKS);

insert into aggfns
select s * 10000::int + t,
    s,
    s,
    case when t % 1051 = 0 then null else (mix(s + t + 1) * 32767)::int2 end,
    (mix(s + t + 2) * 32767 * 65536)::int4,
    (mix(s + t + 3) * 32767 * 65536)::int8,
    case when s = 1 and t = 1061 then 'nan'::float4
        when s = 2 and t = 1061 then '+inf'::float4
        when s = 3 and t = 1061 then '-inf'::float4
        else (mix(s + t + 4) * 100)::float4 end,
    (mix(s + t + 5) * 100)::float8,
    '2021-01-01 01:01:01'::timestamp + interval '1 second' * (s * 10000::int + t),
    '2021-01-01 01:01:01'::timestamptz + interval '1 second' * (s * 10000::int + t),
    '2021-01-01 01:01:01'::timestamptz + interval '1 day' * (s * 10000::int + t)
from
    generate_series(1::int, :CHUNK_ROWS * :CHUNKS / :GROUPING_CARDINALITY) t,
    generate_series(0::int, :GROUPING_CARDINALITY - 1::int) s(s)
;

alter table aggfns set (timescaledb.compress, timescaledb.compress_orderby = 't',
    timescaledb.compress_segmentby = 's');

select count(compress_chunk(x)) from show_chunks('aggfns') x;

analyze aggfns;

---- Uncomment to generate reference. Note that there are minor discrepancies
---- on float4 due to different numeric stability in our and PG implementations.
--set timescaledb.enable_vectorized_aggregation to off;

select
    format('%sselect %s%s(%s) from aggfns%s%s order by 1;',
            explain,
            grouping || ', ',
            function, variable,
            ' where ' || condition,
            ' group by ' || grouping )
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
        'cdate']) variable,
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
    case
--        when explain is not null then condition is null and grouping = 's'
        when explain is not null then false
        when true then true
    end
    and
    case
        when condition = 'cint2 is null' then variable = 'cint2'
        when function = 'count' then variable in ('cfloat4', 's')
        when variable = 't' then function in ('min', 'max')
        when variable in ('cts', 'ctstz', 'cdate') then function in ('min', 'max')
    else true end
order by explain, condition.n, variable, function, grouping.n
\gexec
