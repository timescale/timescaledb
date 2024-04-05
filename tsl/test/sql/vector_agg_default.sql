-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
create function stable_abs(x int4) returns int4 as 'int4abs' language internal stable;

create table t(a int, b int);
select create_hypertable('t', 'a', chunk_time_interval => 1000);

insert into t select x, x % 5 from generate_series(1, 999) x;
alter table t set (timescaledb.compress);
select compress_chunk(show_chunks('t'));

alter table t add column c int default 7;
insert into t select x, x % 5, 11 from generate_series(1001, 1999) x;
select compress_chunk(show_chunks('t'));


-- Just the most basic vectorized aggregation query on a table with default
-- compressed column.
explain (costs off) select sum(c) from t;
select sum(c) from t;


-- Vectorized aggregation should work with vectorized filters.
select sum(c) from t where b >= 0;
select sum(c) from t where b = 0;
select sum(c) from t where b in (0, 1);
select sum(c) from t where b in (0, 1, 3);
select sum(c) from t where b > 10;

explain (costs off) select sum(c) from t where b in (0, 1, 3);


-- The runtime chunk exclusion should work.
explain (costs off) select sum(c) from t where a < stable_abs(1000);


-- Some negative cases.
explain (costs off) select sum(c) from t group by grouping sets ((), (a));

explain (costs off) select sum(c) from t having sum(c) > 0;


-- As a reference, the result on decompressed table.
select decompress_chunk(show_chunks('t'));
select sum(c) from t;

drop table t;
