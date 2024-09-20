-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO errors
create view chunk_info as
with
   chunks as (
      select format('%I.%I', ch.schema_name, ch.table_name)::regclass as chunk,
             format('%I.%I', cc.schema_name, cc.table_name)::regclass as compressed_chunk
        from _timescaledb_catalog.chunk ch
        join _timescaledb_catalog.chunk cc
          on ch.compressed_chunk_id = cc.id),
   parents as (
      select inh.inhparent::regclass as hypertable,
	     cl.oid::regclass as chunk,
	     am.amname
	from pg_class cl
	join pg_am am on cl.relam = am.oid
	join pg_inherits inh on inh.inhrelid = cl.oid)
select hypertable, chunk, compressed_chunk, amname from chunks join parents using (chunk);

\ir include/hyperstore_helpers.sql
\set ECHO all

-- Disable incremental sort to make tests stable
set enable_incremental_sort = false;
select setseed(1);

create table readings(
       time timestamptz unique,
       location int,
       device int,
       temp numeric(4,1),
       humidity float,
       jdata jsonb
);

select create_hypertable('readings', by_range('time', '1d'::interval));

alter table readings
      set (timescaledb.compress_orderby = 'time',
	   timescaledb.compress_segmentby = 'device');

insert into readings (time, location, device, temp, humidity, jdata)
select t, ceil(random()*10), ceil(random()*30), random()*40, random()*100, '{"a":1,"b":2}'::jsonb
from generate_series('2022-06-01'::timestamptz, '2022-06-04'::timestamptz, '5m') t;

select compress_chunk(show_chunks('readings'), compress_using => 'hyperstore');

-- Insert some extra data to get some non-compressed data as well.
insert into readings (time, location, device, temp, humidity, jdata)
select t, ceil(random()*10), ceil(random()*30), random()*40, random()*100, '{"a":1,"b":2}'::jsonb
from generate_series('2022-06-01 00:01:00'::timestamptz, '2022-06-04'::timestamptz, '5m') t;

select chunk, amname from chunk_info where hypertable = 'readings'::regclass;

-- Pick a chunk to truncate that is not the first chunk. This is
-- mostly a precaution to make sure that there is no bias towards the
-- first chunk and we could just as well pick the first chunk.
select chunk from show_chunks('readings') x(chunk) limit 1 offset 3 \gset

-- Check that the number of bytes in the table before and after the
-- truncate.
--
-- Note that a table with a toastable attribute will always have a
-- toast table assigned, so pg_table_size() shows one page allocated
-- since this includes the toast table.
select pg_table_size(chunk) as chunk_size,
       pg_table_size(compressed_chunk) as compressed_chunk_size
  from chunk_info
 where chunk = :'chunk'::regclass;
truncate :chunk;
select pg_table_size(chunk) as chunk_size,
       pg_table_size(compressed_chunk) as compressed_chunk_size
  from chunk_info
 where chunk = :'chunk'::regclass;

-- We test TRUNCATE on a hypertable as well, but truncating a
-- hypertable is done by deleting all chunks, not by truncating each
-- chunk.
select (select count(*) from readings) tuples,
       (select count(*) from show_chunks('readings')) chunks;
truncate readings;
select (select count(*) from readings) tuples,
       (select count(*) from show_chunks('readings')) chunks;


