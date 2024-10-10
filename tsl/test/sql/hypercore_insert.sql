-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hypercore.sql

-- Compress the chunks and check that the counts are the same
select location_id, count(*) into orig from :hypertable GROUP BY location_id;
select compress_chunk(show_chunks(:'hypertable'), compress_using => 'hypercore');
select location_id, count(*) into comp from :hypertable GROUP BY location_id;
select * from orig join comp using (location_id) where orig.count != comp.count;
drop table orig, comp;

-- Check that all chunks are compressed
select chunk_name, compression_status from chunk_compression_stats(:'hypertable');

--
-- Test that a conflict happens when inserting a value that already
-- exists in the compressed part of the chunk.
--

-- Check that we have at least one timestamp that we want to test.
SELECT count(*) FROM :chunk1 WHERE created_at  = '2022-06-01'::timestamptz;

\set ON_ERROR_STOP 0
insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01', 1, 1, 1.0, 1.0);

insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01', 1, 1, 1.0, 1.0);
\set ON_ERROR_STOP 1

-- Insert values, which will end up in the uncompressed parts
INSERT INTO :chunk1(created_at, location_id, device_id, temp, humidity)
VALUES ('2022-06-01 00:00:02', 1, 1, 1.0, 1.0);

INSERT INTO :hypertable(created_at, location_id, device_id, temp, humidity)
VALUES ('2022-06-01 00:00:03', 1, 1, 1.0, 1.0);

-- Should still generate an error
\set ON_ERROR_STOP 0
insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:02', 1, 1, 1.0, 1.0);

insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:02', 1, 1, 1.0, 1.0);

insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:03', 1, 1, 1.0, 1.0);

insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:03', 1, 1, 1.0, 1.0);
\set ON_ERROR_STOP 1

set session characteristics as transaction isolation level repeatable read;

--
-- Testing speculative inserts and upserts
--

-- Speculative insert with conflicting row should succeed and not
-- insert anything

select location_id, count(*) into orig from :hypertable GROUP BY location_id;

-- Inserting into compressed part
insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01', 1, 1, 1.0, 1.0)
on conflict (created_at) do nothing;

insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01', 1, 1, 1.0, 1.0)
on conflict (created_at) do nothing;

-- Inserting into non-compressed part
insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:02', 1, 1, 1.0, 1.0)
on conflict (created_at) do nothing;

insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:02', 1, 1, 1.0, 1.0)
on conflict (created_at) do nothing;

insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:03', 1, 1, 1.0, 1.0)
on conflict (created_at) do nothing;

insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:03', 1, 1, 1.0, 1.0)
on conflict (created_at) do nothing;

-- Check the count after the operations above. We count each location
-- here since it is easier to see what went wrong if something did.
select location_id, count(*) into curr from :hypertable GROUP BY location_id;
select * from :hypertable where created_at between '2022-06-01 00:00:01' and '2022-06-01 00:00:09';
select * from orig join curr using (location_id) where orig.count != curr.count;
drop table curr;

-- Speculative insert with non-conflicting rows should succeed.

insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:06', 1, 1, 1.0, 1.0)
on conflict (created_at) do nothing;

insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:07', 1, 1, 1.0, 1.0)
on conflict (created_at) do nothing;

select location_id, count(*) into curr from :hypertable GROUP BY location_id;
select location_id, curr.count - orig.count as increase
  from orig join curr using (location_id)
 where orig.count != curr.count;
drop table orig, curr;


-- Upserts with conflicting and non-conflicting rows should work
-- correctly also when inserting directly into the chunks.
--
-- We have tested this above, but since different code paths are used
-- for DO UPDATE, DO NOTHING, and plain inserts, we test this as well
-- to be safe.

-- find a compressed tuple in a deterministic manner and get the
-- timestamp. Make sure to find one in chunk1 since we will use that
-- later.
select created_at
from :chunk1 where _timescaledb_debug.is_compressed_tid(ctid)
order by created_at limit 1 \gset

select * from :hypertable where created_at = :'created_at';

-- Insert of a value that exists in the compressed part should work
-- when done through the hypertable.
insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values (:'created_at', 11, 1, 1.0, 1.0)
on conflict (created_at) do update set location_id = 12;

select * from :hypertable where created_at = :'created_at';

-- TODO(timescale/timescaledb-private#1087): Inserts directly into a
-- compressed tuple in a chunk do not work.

select created_at
from :chunk1 where _timescaledb_debug.is_compressed_tid(ctid)
order by created_at limit 1 \gset

\set ON_ERROR_STOP 0
insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values (:'created_at', 13, 1, 1.0, 1.0)
on conflict (created_at) do update set location_id = 14;
\set ON_ERROR_STOP 1

-- Insert of a value that exists in the non-compressed part. (These
-- were inserted above.)
insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:02', 15, 1, 1.0, 1.0)
on conflict (created_at) do update set location_id = 16;

insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:03', 17, 1, 1.0, 1.0)
on conflict (created_at) do update set location_id = 18;

-- Inserting them again should still update.
insert into :chunk1(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:02', 19, 1, 1.0, 1.0)
on conflict (created_at) do update set location_id = 20;

insert into :hypertable(created_at, location_id, device_id, temp, humidity)
values ('2022-06-01 00:00:03', 21, 1, 1.0, 1.0)
on conflict (created_at) do update set location_id = 22;

select * from :hypertable where location_id between 11 and 22
order by location_id;

drop table :hypertable;

-- Check that we can write to a hypercore table from another kind of
-- slot even if we have dropped and added attributes.
create table test2 (itime integer, b bigint, t text);
select create_hypertable('test2', by_range('itime', 10));

create table test2_source(itime integer, d int, t text);
insert into test2_source values (9, '9', 90), (17, '17', 1700);


-- this will create a single chunk.
insert into test2 select t, 10,  'first'::text from generate_series(1, 7) t;

alter table test2 drop column b;
alter table test2 add column c int default -15;
alter table test2 add column d int;

-- Since we have chunk sizes of 10, this will create a second chunk
-- with a second set of attributes where one is dropped.
insert into test2 select t, 'second'::text, 120, 1 from generate_series(11, 15) t;

alter table test2
      set access method hypercore,
      set (timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'c, itime desc');

select compress_chunk(show_chunks('test2'));

-- Insert into both chunks using a select.
insert into test2(itime ,t , d) select itime, t, d from test2_source;
