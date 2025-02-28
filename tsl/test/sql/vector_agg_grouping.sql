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

create table agggroup(t int, s int,
    cint2 int2, cint4 int4, cint8 int8);
select create_hypertable('agggroup', 's', chunk_time_interval => :GROUPING_CARDINALITY / :CHUNKS);

create view source as
select s * 10000 + t as t,
    s,
    case when t % 1051 = 0 then null
        else (mix(s + t * 1019) * 32767)::int2 end as cint2,
    (mix(s + t * 1021) * 32767)::int4 as cint4,
    (mix(s + t * 1031) * 32767)::int8 as cint8
from
    generate_series(1::int, :CHUNK_ROWS * :CHUNKS / :GROUPING_CARDINALITY) t,
    generate_series(0::int, :GROUPING_CARDINALITY - 1::int) s(s)
;

insert into agggroup select * from source where s = 1;

alter table agggroup set (timescaledb.compress, timescaledb.compress_orderby = 't',
    timescaledb.compress_segmentby = 's');

select count(compress_chunk(x)) from show_chunks('agggroup') x;

alter table agggroup add column ss int default 11;
alter table agggroup add column x text default '11';

insert into agggroup
select *, ss::text as x from (
    select *,
        case
            -- null in entire batch
            when s = 2 then null
            -- null for some rows
            when s = 3 and t % 1051 = 0 then null
            -- for some rows same as default
            when s = 4 and t % 1057 = 0 then 11
            -- not null for entire batch
            else s
        end as ss
    from source where s != 1
) t
;
select count(compress_chunk(x)) from show_chunks('agggroup') x;
vacuum freeze analyze agggroup;


set timescaledb.debug_require_vector_agg = 'require';
---- Uncomment to generate reference. Note that there are minor discrepancies
---- on float4 due to different numeric stability in our and PG implementations.
--set timescaledb.enable_vectorized_aggregation to off; set timescaledb.debug_require_vector_agg = 'allow';

select
    format('%sselect %s%s(%s) from agggroup%s%s%s;',
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
        'cint2',
        '*']) variable,
    unnest(array[
        'min',
        'count']) function,
    unnest(array[
        null,
        'cint2 > 0',
        'cint2 is null',
        'cint2 is null and x is null']) with ordinality as condition(condition, n),
    unnest(array[
        null,
        'cint2',
        'cint4',
        'cint4, cint8',
        'cint8',
        's, cint2',
        's, ss',
        's, x',
        'ss, cint2, x',
        'ss, s',
        'ss, x, cint2',
        't, s, ss, x, cint4, cint8, cint2',
        'x']) with ordinality as grouping(grouping, n)
where
    true
    and (explain is null /* or condition is null and grouping = 's' */)
    and (variable != '*' or function = 'count')
order by explain, condition.n, variable, function, grouping.n
\gexec

reset timescaledb.debug_require_vector_agg;


-- Test long text columns. Also make one of them a segmentby, so that we can
-- test the long scalar values.
create table long(t int, a text, b text, c text, d text);
select create_hypertable('long', 't');
insert into long select n, a, x, x, x from (
    select n, 'short' || m a, repeat('1', 100 * 4 + n) x
    from generate_series(1, 4) n,
        generate_series(1, 4) m) t
;
insert into long values (-1, 'a', 'b', 'c', 'd');
insert into long values (-2, repeat('long', 1000), 'b', 'c', 'd');
alter table long set (timescaledb.compress, timescaledb.compress_segmentby = 'a',
    timescaledb.compress_orderby = 't desc');
select count(compress_chunk(x)) from show_chunks('long') x;

set timescaledb.debug_require_vector_agg = 'require';
---- Uncomment to generate reference.
--set timescaledb.enable_vectorized_aggregation to off; set timescaledb.debug_require_vector_agg = 'allow';

-- Various placements of long scalar column
select sum(t) from long group by a, b, c, d order by 1 limit 10;
select sum(t) from long group by d, b, c, a order by 1 limit 10;
select sum(t) from long group by d, b, a, c order by 1 limit 10;

-- Just the scalar column
select sum(t) from long group by a order by 1;

-- No scalar columns
select sum(t) from long group by b, c, d order by 1 limit 10;

reset timescaledb.debug_require_vector_agg;


-- Test various serialized key lengths. We want to touch the transition from short
-- to long varlena header for the serialized key.
create table keylength(t int, a text, b text);
select create_hypertable('keylength', 't');
insert into keylength select t, 'a', repeat('b', t) from generate_series(1, 1000) t;
insert into keylength values (-1, '', ''); -- second chunk
alter table keylength set (timescaledb.compress, timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 't desc');
select count(compress_chunk(x)) from show_chunks('keylength') x;

set timescaledb.debug_require_vector_agg = 'require';
---- Uncomment to generate reference.
--set timescaledb.enable_vectorized_aggregation to off; set timescaledb.debug_require_vector_agg = 'allow';

select sum(t) from keylength group by a, b order by 1 desc limit 10;
select sum(t) from keylength group by b, a order by 1 desc limit 10;

reset timescaledb.debug_require_vector_agg;
