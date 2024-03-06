-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table sparse(ts int, value float);
select create_hypertable('sparse', 'ts');
insert into sparse select x, x from generate_series(1, 10000) x;
alter table sparse set (timescaledb.compress);

-- When the chunks are compressed, minmax metadata are created for columns that
-- have btree indexes.
create index ii on sparse(value);
select count(compress_chunk(x)) from show_chunks('sparse') x;
explain select * from sparse where value = 1;


-- Should be disabled with the GUC
set timescaledb.auto_sparse_indexes to off;
select count(compress_chunk(decompress_chunk(x))) from show_chunks('sparse') x;
explain select * from sparse where value = 1;
reset timescaledb.auto_sparse_indexes;
select count(compress_chunk(decompress_chunk(x))) from show_chunks('sparse') x;
explain select * from sparse where value = 1;


-- Should survive renames.
alter table sparse rename column value to wert;
explain select * from sparse where wert = 1;
alter table sparse rename column wert to value;
explain select * from sparse where value = 1;


-- Not for expression indexes.
drop index ii;
create index ii on sparse((value + 1));
select count(compress_chunk(decompress_chunk(x))) from show_chunks('sparse') x;
explain select * from sparse where value = 1;


-- Not for other index types.
drop index ii;
create index ii on sparse using hash(value);
select count(compress_chunk(decompress_chunk(x))) from show_chunks('sparse') x;
explain select * from sparse where value = 1;


-- When the chunk is recompressed without index, no sparse index is created.
drop index ii;
select count(compress_chunk(decompress_chunk(x))) from show_chunks('sparse') x;
explain select * from sparse where value = 1;


-- Long column names.
select count(decompress_chunk(x)) from show_chunks('sparse') x;

\set ECHO none
select format('alter table sparse add column %1$s int; create index on sparse(%1$s);',
    substr('Abcdef012345678_Bbcdef012345678_Cbcdef012345678_Dbcdef012345678_', 1, x))
from generate_series(1, 63) x
\gexec
\set ECHO queries

select count(compress_chunk(x)) from show_chunks('sparse') x;

explain select * from sparse where Abcdef012345678_Bbcdef012345678_Cbcdef012345678_Dbcdef0 = 1;

