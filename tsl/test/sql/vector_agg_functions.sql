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
    cfloat4 float4, cfloat8 float8);
select create_hypertable('aggfns', 's', chunk_time_interval => :GROUPING_CARDINALITY / :CHUNKS);

insert into aggfns
select s * 10000::int + t,
    s,
    s,
    case when t % 1051 = 0 then null else (mix(s + t + 1) * 32767)::int2 end,
    (mix(s + t + 2) * 32767 * 65536)::int4,
    (mix(s + t + 3) * 32767 * 65536)::int8,
    case when s = 1 and t = 1061 then 'nan'::float4
        else (mix(s + t + 4) * 100)::float4 end,
    (mix(s + t + 5) * 100)::float8
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
    format('select %4$s, %1$s(%2$s) from aggfns where %3$s group by %4$s order by 1, 2;',
            function, variable, condition, grouping)
from
    unnest(array[
        't',
        's',
        'ss',
        'cint2',
        'cint4',
        'cint8',
        'cfloat4',
        'cfloat8']) variable,
    unnest(array[
        'min',
        'max',
        'sum',
        'avg',
        'count']) function,
    unnest(array[
        'true',
        'cfloat8 > 0',
        'cfloat8 <= 0',
        'cfloat8 < 1000' /* vectorized qual is true for all rows */,
        'cfloat8 > 1000' /* vectorized qual is false for all rows */,
        'cint2 is null']) with ordinality as condition(condition, n),
    unnest(array[
        '777::text' /* dummy grouping column */,
        's',
        'ss']) grouping
where
    case
        when condition = 'cint2 is null' then variable = 'cint2'
        when function = 'count' then variable = 'cfloat4'
        when variable = 't' then function in ('min', 'max')
        else true end
order by condition.n, variable, function, grouping
\gexec
