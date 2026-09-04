-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Verify that creating a compressed chunk records a pg_depend dependency on
-- its hypertable, and that decompressing, dropping the chunk or the hypertable
-- cleans it up correctly.
create table test (time timestamptz not null, device text, value float8);
select create_hypertable('test', 'time');
insert into test
select '2024-01-01'::timestamptz + (i || ' hour')::interval, 'd' || (i % 3), i
from generate_series(1, 50) i;
alter table test set (timescaledb.compress, timescaledb.compress_segmentby = 'device');

select count(compress_chunk(x)) from show_chunks('test') x;

select compress_relid::oid as compressed_chunk_oid
from _timescaledb_catalog.compression_settings
where compress_relid is not null \gset

-- a pg_depend row should link the compressed chunk relation to the hypertable
select classid::regclass, refclassid::regclass, refobjid::regclass, deptype
from pg_depend
where classid = 'pg_class'::regclass
  and objid = :compressed_chunk_oid
  and refclassid = 'pg_class'::regclass
  and refobjid = 'test'::regclass;

-- decompressing drops the compressed relation and its pg_depend row
select count(decompress_chunk(x)) from show_chunks('test') x;

select count(*) from pg_depend where objid = :compressed_chunk_oid;
select count(*) from pg_class where oid = :compressed_chunk_oid;

-- recompress
alter table test set (timescaledb.compress);
select count(compress_chunk(x)) from show_chunks('test') x;

select compress_relid::oid as compressed_chunk_oid
from _timescaledb_catalog.compression_settings
where compress_relid is not null \gset

-- dropping a chunk deletes its compressed chunk's pg_depend row
begin;
select _timescaledb_functions.drop_chunk(x) from show_chunks('test') x;
select count(*) from pg_depend where objid = :compressed_chunk_oid;
rollback;

-- dropping the hypertable deletes the compressed chunk's pg_depend row
drop table test;

select count(*) from pg_depend where objid = :compressed_chunk_oid;
select count(*) from pg_class where oid = :compressed_chunk_oid;
