-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
create function stable_abs(x int4) returns int4 as 'int4abs' language internal stable;

create table dvagg(a int, b int);
select create_hypertable('dvagg', 'a', chunk_time_interval => 1000);

insert into dvagg select x, x % 5 from generate_series(1, 999) x;
alter table dvagg set (timescaledb.compress);
select compress_chunk(show_chunks('dvagg'));

alter table dvagg add column c int default 7;
insert into dvagg select x, x % 5, 11 from generate_series(1001, 1999) x;
select compress_chunk(show_chunks('dvagg'));


-- Just the most basic vectorized aggregation query on a table with default
-- compressed column.
explain (costs off) select sum(c) from dvagg;
select sum(c) from dvagg;


---- Uncomment to generate reference.
--set timescaledb.enable_vectorized_aggregation to off;

-- Vectorized aggregation should work with vectorized filters.
select sum(c) from dvagg where b >= 0;
select sum(c) from dvagg where b = 0;
select sum(c) from dvagg where b in (0, 1);
select sum(c) from dvagg where b in (0, 1, 3);
select sum(c) from dvagg where b > 10;

select count(*) from dvagg where b >= 0;
select count(*) from dvagg where b = 0;
select count(*) from dvagg where b in (0, 1);
select count(*) from dvagg where b in (0, 1, 3);
select count(*) from dvagg where b > 10;

explain (costs off) select sum(c) from dvagg where b in (0, 1, 3);

select sum(a), sum(b), sum(c) from dvagg where b in (0, 1, 3);

explain (costs off) select sum(a), sum(b), sum(c) from dvagg where b in (0, 1, 3);

reset timescaledb.enable_vectorized_aggregation;


-- The runtime chunk exclusion should work.
explain (costs off) select sum(c) from dvagg where a < stable_abs(1000);


-- Some negative cases.
explain (costs off) select sum(c) from dvagg group by grouping sets ((), (a));

explain (costs off) select sum(c) from dvagg having sum(c) > 0;


-- As a reference, the result on decompressed table.
select decompress_chunk(show_chunks('dvagg'));
select sum(c) from dvagg;

drop table dvagg;
