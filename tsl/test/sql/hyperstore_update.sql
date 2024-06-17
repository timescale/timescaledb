-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

\ir include/setup_hyperstore.sql

-- TODO(#1068) Parallel sequence scan does not work
set max_parallel_workers_per_gather to 0;

select compress_chunk(show_chunks(:'hypertable'), compress_using => 'hyperstore');

-- Check that all chunks are compressed
select chunk_name, compression_status from chunk_compression_stats(:'hypertable');

select relname, amname
  from pg_class join pg_am on (relam = pg_am.oid)
 where pg_class.oid in (select show_chunks(:'hypertable'))
order by relname;

-- Pick a random row to update
\x on
select * from :hypertable order by created_at offset 577 limit 1;
select created_at, location_id, owner_id, device_id
from :hypertable order by created_at offset 577 limit 1 \gset
\x off

-- Test updating the same row using segment-by column, other lookup column
explain (costs off)
update :hypertable set temp = 100.0 where created_at = :'created_at';
\x on
select * from :hypertable where created_at = :'created_at';
update :hypertable set temp = 100.0 where created_at = :'created_at';
select * from :hypertable where created_at = :'created_at';
\x off

-- Disable checks on max tuples decompressed per transaction
set timescaledb.max_tuples_decompressed_per_dml_transaction to 0;

-- Using the segmentby attribute that has an index
explain (costs off)
update :hypertable set humidity = 110.0 where location_id = :location_id;
select count(*) from :hypertable where humidity = 110.0;
update :hypertable set humidity = 110.0 where location_id = :location_id;
select count(*) from :hypertable where humidity = 110.0;

-- Make sure there is a mix of compressed and non-compressed rows for
-- the select for update test below. Run an update on a metric_id to
-- decompress the corresponding segment.
update :hypertable set humidity = 120.0 where metric_id = 1001;

-- Testing to select for update, and then perform an update of those
-- rows. The selection is to make sure that we have a mix of
-- compressed and uncompressed tuples.
start transaction;
select _timescaledb_debug.is_compressed_tid(ctid),
       metric_id, created_at
  into to_update
  from :hypertable
 where metric_id between 6330 and 6340
order by metric_id for update;

select * from to_update order by metric_id;

update :hypertable set humidity = 200.0, temp = 500.0
where (created_at, metric_id) in (select created_at, metric_id from to_update);

select * from :hypertable where humidity = 200.0 order by metric_id;
commit;

-----------------------------
-- Test update from cursor --
-----------------------------

-- Test cursor update via hypertable. Data is non-compressed
\x on
select _timescaledb_debug.is_compressed_tid(ctid), *
from :hypertable order by created_at offset 898 limit 1;
select created_at, location_id, owner_id, device_id, humidity
from :hypertable order by created_at offset 898 limit 1 \gset
\x off

begin;
declare curs1 cursor for select humidity from :hypertable where created_at = :'created_at' for update;
fetch forward 1 from curs1;

-- Update via the cursor. The update should work since it happens on
-- non-compressed data
update :hypertable set humidity = 200.0 where current of curs1;
commit;
select humidity from :hypertable
where created_at = :'created_at' and humidity = 200.0;

-- Test cursor update via hypertable on compressed data
--
-- First, make sure the data is compressed. Do it only on the chunk we
-- will select the cursor from to make it faster
select ch as chunk
from show_chunks(:'hypertable') ch limit 1 \gset
vacuum full :chunk;

\x on
select _timescaledb_debug.is_compressed_tid(ctid), *
from :hypertable order by created_at offset 898 limit 1;
select created_at, location_id, owner_id, device_id, humidity
from :hypertable order by created_at offset 898 limit 1 \gset
\x off

begin;
declare curs1 cursor for select humidity from :hypertable where created_at = :'created_at' for update;
fetch forward 1 from curs1;
update :hypertable set humidity = 400.0 where current of curs1;
commit;
-- The update silently succeeds but doesn't update anything since DML
-- decompression deleted the row at the cursor and moved it to the
-- non-compressed rel. Currently, this is not the "correct" behavior.
select humidity from :hypertable where created_at = :'created_at' and humidity = 400.0;

\x on
select _timescaledb_debug.is_compressed_tid(ctid), *
from :hypertable order by created_at offset 898 limit 1;
select created_at, location_id, owner_id, device_id, humidity
from :hypertable order by created_at offset 898 limit 1 \gset
\x off

-- Test doing the update directly on the chunk. The data should now be
-- decompressed again due to DML decompression in the previous query.
begin;
declare curs1 cursor for select humidity from :chunk where created_at = :'created_at' for update;
fetch forward 1 from curs1;
update :chunk set humidity = 400.0 where current of curs1;
commit;
-- The update should succeed because the data is not compressed
select humidity from :chunk where created_at = :'created_at' and humidity = 400.0;

-- Recompress everything again and try cursor update via chunk on
-- compressed data
vacuum full :chunk;
\set ON_ERROR_STOP 0
begin;
declare curs1 cursor for select humidity from :chunk where created_at = :'created_at' for update;
fetch forward 1 from curs1;
-- The update should now "correctly" fail with an error when it
-- happens on compressed data.
update :chunk set humidity = 500.0 where current of curs1;
commit;
\set ON_ERROR_STOP 1
