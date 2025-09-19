-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

-- Uncomment these two settings to run this test with hypercore TAM
--set timescaledb.default_hypercore_use_access_method=true;
--set enable_indexscan=off;

create function stable_abs(x int4) returns int4 as 'int4abs' language internal stable;

create table dvagg(a int, b int);
select create_hypertable('dvagg', 'a', chunk_time_interval => 1000);

insert into dvagg select x, x % 5 from generate_series(1, 999) x;
alter table dvagg set (timescaledb.compress);
select compress_chunk(show_chunks('dvagg'));

alter table dvagg add column c int default 7;
insert into dvagg select x, x % 5, 11 from generate_series(1001, 1999) x;
select compress_chunk(show_chunks('dvagg'));

vacuum analyze dvagg;


-- Just the most basic vectorized aggregation query on a table with default
-- compressed column.
explain (buffers off, costs off) select sum(c) from dvagg;
select sum(c) from dvagg;


set timescaledb.debug_require_vector_agg = 'require';
---- Uncomment to generate reference
--set timescaledb.debug_require_vector_agg = 'forbid';
--set timescaledb.enable_vectorized_aggregation to off;

-- Test vectorized aggregation, filters and expressions with a default column.
select
    format('select %s%s from dvagg%s%s%s;',
            grouping || ', ',
            function,
            ' where ' || condition,
            ' group by ' || grouping,
            format(' order by %s, ', function) || grouping || ' limit 10')
from
    unnest(array[
        'count(*)',
        'sum(c)',
        'sum(b + c)']) function,
    unnest(array[
        null,
        'b >= 0',
        'b = 0',
        'b in (0, 1)',
        'b in (0, 1, 3)',
        'b > 10',
        'c != 7',
        'c > 1000',
        'c < 1000']) with ordinality as condition(condition, n),
    unnest(array[
        null,
        'b',
        'c',
        'b + c']) with ordinality as grouping(grouping, n)
\gexec


explain (buffers off, costs off) select sum(c) from dvagg where b in (0, 1, 3);

select sum(a), sum(b), sum(c) from dvagg where b in (0, 1, 3);

explain (buffers off, costs off) select sum(a), sum(b), sum(c) from dvagg where b in (0, 1, 3);

reset timescaledb.enable_vectorized_aggregation;


-- The runtime chunk exclusion should work.
explain (buffers off, costs off) select sum(c) from dvagg where a < stable_abs(1000);

-- The case with HAVING can still be vectorized because it is applied after
-- final aggregation.
select sum(c) from dvagg having sum(c) > 0;


-- Some negative cases.
set timescaledb.debug_require_vector_agg to 'forbid';

explain (buffers off, costs off) select sum(c) from dvagg group by grouping sets ((), (a));



-- As a reference, the result on decompressed table.
select decompress_chunk(show_chunks('dvagg'));
select sum(c) from dvagg;

drop table dvagg;
