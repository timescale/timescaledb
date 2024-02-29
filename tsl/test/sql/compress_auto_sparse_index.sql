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
