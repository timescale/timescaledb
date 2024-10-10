-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

select setseed(0.3);
create table devices (id int, primary key (id));
create table locations (id int, primary key (id));
create table metrics (time timestamptz, device int, location int, temp float);
select create_hypertable('metrics', 'time', create_default_indexes => false);

insert into devices values (1), (2);
insert into locations values (1), (2);
insert into metrics values ('2024-01-01', 1, 1, 1.0), ('2024-01-01', 2, 2, 2.0), ('2024-01-02', 1, 2, 3.0), ('2024-01-02', 2, 2, 4.0);
-- Add foreign key constraint to test cascading deletes
alter table metrics add constraint device_fk foreign key (device) references devices (id) on delete cascade;
alter table metrics set (timescaledb.compress_segmentby = 'device');

-- Make the one chunk a Hypercore
select ch as chunk from show_chunks('metrics') ch limit 1 \gset
alter table :chunk set access method hypercore;

-- Show that all data is compressed
select _timescaledb_debug.is_compressed_tid(ctid) as compressed, * from metrics order by time, device;

\set ON_ERROR_STOP 0
-- It should not be possible to do non-whole segment deletes
delete from :chunk where location=1;
delete from :chunk where location=2;
delete from :chunk where device=1 and location=2;
delete from :chunk where temp=1;
\set ON_ERROR_STOP 1

start transaction;
-- Deleting whole segment is OK
delete from :chunk where device=1;
select _timescaledb_debug.is_compressed_tid(ctid) as compressed, * from metrics order by time, device;
rollback;

start transaction;
-- Deleting multiple whole segments is also OK
delete from :chunk where device in (1,2);
select _timescaledb_debug.is_compressed_tid(ctid) as compressed, * from metrics order by time, device;
rollback;
start transaction;
-- It is possible to delete by location as long as whole segments are
-- deleted
delete from :chunk where location in (1,2);
select _timescaledb_debug.is_compressed_tid(ctid) as compressed, * from metrics order by time, device;
rollback;

-- Test delete via hypertable. It will lead to DML decompression, so
-- not whole-segment delete.
start transaction;
delete from metrics where location=1;
select _timescaledb_debug.is_compressed_tid(ctid) as compressed, * from metrics order by time, device;
rollback;
select _timescaledb_debug.is_compressed_tid(ctid) as compressed, * from metrics order by time, device;

-----------------------------------------------------
-- Test cascading deletes via foreign key constraint
-----------------------------------------------------

-- Delete from devices table should cascade via foreign key to metrics
-- table. First show that data exists and is compressed
explain
delete from devices where id=2;

start transaction;
delete from devices where id=2;
-- No rows for device 2 should remain
select _timescaledb_debug.is_compressed_tid(ctid) as compressed, * from metrics where device=2 order by time, device;
rollback;

-----------------------------------------------------
-- Try deletes on bigger data set with full segments
-----------------------------------------------------
insert into devices values (3), (4);
insert into metrics (time, device, location, temp)
select t, ceil(random()*4), ceil(random()*30), random()*40
from generate_series('2024-01-01'::timestamptz, '2024-01-03', '10s') t;

-- Still just one chunk
select count(ch) from show_chunks('metrics') ch;

-- Make sure it is fully compressed
vacuum full metrics;

-- Find the compressed chunk rel
create view compressed_rels as
with reg_chunk as (
	 select * from _timescaledb_catalog.chunk where compressed_chunk_id IS NOT NULL
)
select format('%I.%I', reg_chunk.schema_name, reg_chunk.table_name)::regclass as relid,
	   format('%I.%I', cpr_chunk.schema_name, cpr_chunk.table_name)::regclass as compressed_relid
from _timescaledb_catalog.chunk cpr_chunk
inner join reg_chunk on (cpr_chunk.id = reg_chunk.compressed_chunk_id);

select compressed_relid as cchunk from compressed_rels \gset
select _ts_meta_count, device from :cchunk
where device in (1, 2, 3)
order by device, _ts_meta_count;

start transaction;
delete from devices where id=2;
-- No rows for device 2 should remain
select _ts_meta_count, device from :cchunk
where device in (1, 2, 3)
order by device, _ts_meta_count;
select _timescaledb_debug.is_compressed_tid(ctid) as compressed, * from metrics where device=2 order by time, device;
rollback;

-- Insert non-compressed rows
insert into metrics values ('2024-01-01', 1, 10, 6.0), ('2024-01-01', 2, 12, 7.0), ('2024-01-02', 1, 10, 8.0), ('2024-01-02', 2, 13, 9.0);

start transaction;
-- Delete directly on chunk instead of cascading FK delete
delete from :chunk where device=1;
-- No rows for device 1 should remain, neither compressed nor non-compressed
select _ts_meta_count, device from :cchunk
where device=1
order by device, _ts_meta_count;
select _timescaledb_debug.is_compressed_tid(ctid) as compressed, * from metrics where device=1 order by time, device;
rollback;

select count(*) from metrics;
-- Try deleting on a device that doesn't exist
delete from metrics where device=10;
-- Nothing should be deleted and count() should be the same
select count(*) from metrics;
