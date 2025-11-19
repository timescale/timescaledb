-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table hashes(ts int,
    x text,
    u uuid,
    d date,
    i4 int4,
    i8 int8)
with (tsdb.hypertable, tsdb.partition_column = 'ts',
    tsdb.compress, tsdb.orderby = 'ts',
    tsdb.sparse_index = 'bloom(x), bloom(u), bloom(d), bloom(i4), bloom(i8)'
)
;

insert into hashes
select 1,
    'test text',
    '90ec9e8e-4501-4232-9d03-6d7cf6132815',
    '2021-01-01',
    '1435',
    '30064771065'
;

select count(compress_chunk(x)) from show_chunks('hashes') x;
vacuum full analyze hashes;

select schema_name || '.' || table_name chunk from _timescaledb_catalog.chunk
    where id = (select compressed_chunk_id from _timescaledb_catalog.chunk
        where hypertable_id = (select id from _timescaledb_catalog.hypertable
            where table_name = 'hashes') limit 1)
\gset

\pset format unaligned
\pset expanded on
select * from :chunk;
\pset format aligned
\pset expanded off
