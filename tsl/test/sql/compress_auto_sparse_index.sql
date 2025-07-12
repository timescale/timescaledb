-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table sparse(ts int, value float);
select create_hypertable('sparse', 'ts');
insert into sparse select x, x from generate_series(1, 10000) x;

-- When the chunks are compressed, minmax metadata are created for columns that
-- have btree indexes.
create index ii on sparse(value);
alter table sparse set (timescaledb.compress, timescaledb.segment_by='', timescaledb.order_by='ts desc');
select count(compress_chunk(x)) from show_chunks('sparse') x;
vacuum analyze sparse;
explain (costs off) select * from sparse where value = 1;
drop table sparse;

-- Should be disabled with the GUC
set timescaledb.auto_sparse_indexes to off;
create table sparse(ts int, value float);
select create_hypertable('sparse', 'ts');
insert into sparse select x, x from generate_series(1, 10000) x;
create index ii on sparse(value);
alter table sparse set (timescaledb.compress, timescaledb.segment_by='', timescaledb.order_by='ts desc');
select count(compress_chunk(x)) from show_chunks('sparse') x;
vacuum analyze sparse;
explain (costs off) select * from sparse where value = 1;
drop table sparse;
reset timescaledb.auto_sparse_indexes;

create table sparse(ts int, value float);
select create_hypertable('sparse', 'ts');
insert into sparse select x, x from generate_series(1, 10000) x;
create index ii on sparse(value);
alter table sparse set (timescaledb.compress, timescaledb.segment_by='', timescaledb.order_by='ts desc');
select count(compress_chunk(x)) from show_chunks('sparse') x;
vacuum analyze sparse;
explain (costs off) select * from sparse where value = 1;


-- Should survive renames.
alter table sparse rename column value to wert;
explain (costs off) select * from sparse where wert = 1;
alter table sparse rename column wert to value;
explain (costs off) select * from sparse where value = 1;
drop table sparse;

-- Not for expression indexes.
create table sparse(ts int, value float);
select create_hypertable('sparse', 'ts');
insert into sparse select x, x from generate_series(1, 10000) x;
create index ii on sparse((value + 1));
alter table sparse set (timescaledb.compress, timescaledb.segment_by='', timescaledb.order_by='ts desc');
select count(compress_chunk(x)) from show_chunks('sparse') x;
vacuum analyze sparse;
explain (costs off) select * from sparse where value = 1;
drop table sparse;

-- Not for other index types.
create table sparse(ts int, value float);
select create_hypertable('sparse', 'ts');
insert into sparse select x, x from generate_series(1, 10000) x;
create index ii on sparse using hash(value);
alter table sparse set (timescaledb.compress, timescaledb.segment_by='', timescaledb.order_by='ts desc');
select count(compress_chunk(x)) from show_chunks('sparse') x;
vacuum analyze sparse;
explain (costs off) select * from sparse where value = 1;
drop table sparse;


-- When the chunk is recompressed without index, no sparse index is created.
create table sparse(ts int, value float);
select create_hypertable('sparse', 'ts');
insert into sparse select x, x from generate_series(1, 10000) x;
alter table sparse set (timescaledb.compress, timescaledb.segment_by='', timescaledb.order_by='ts desc');
select count(compress_chunk(x)) from show_chunks('sparse') x;
vacuum analyze sparse;
explain (costs off) select * from sparse where value = 1;
drop table sparse;

-- Long column names.
create table sparse(ts int, value float);
select create_hypertable('sparse', 'ts');
insert into sparse select x, x from generate_series(1, 10000) x;
\set ECHO none
select format('alter table sparse add column %1$s bigint; create index on sparse(%1$s);',
    substr('Abcdef012345678_Bbcdef012345678_Cbcdef012345678_Dbcdef012345678_', 1, x))
from generate_series(1, 63) x
\gexec
\set ECHO queries
alter table sparse set (timescaledb.compress, timescaledb.segment_by='', timescaledb.order_by='ts desc');
select count(compress_chunk(x)) from show_chunks('sparse') x;
vacuum analyze sparse;

explain (costs off) select * from sparse where Abcdef012345678_Bbcdef012345678_Cbcdef012345678_Dbcdef0 = 1;

