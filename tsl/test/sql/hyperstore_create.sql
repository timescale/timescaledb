-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Testing the basic API for creating a hyperstore

CREATE TABLE test2(
       created_at timestamptz not null,
       location_id int,
       device_id int,
       temp float,
       humidity float
);

create index on test2(device_id, created_at);

\set ON_ERROR_STOP 0
alter table test2 set access method hyperstore;
\set ON_ERROR_STOP 1

select create_hypertable('test2', 'created_at');

\set ON_ERROR_STOP 0
-- Should show error since there is no namespace.
alter table test2
      set access method hyperstore,
      set (compress_segmentby = 'location_id');
\set ON_ERROR_STOP 1

alter table test2
      set access method hyperstore,
      set (timescaledb.compress_segmentby = 'location_id');

-- Test altering hypertable to hyperstore again. It should be allowed
-- and be a no-op.
alter table test2 set access method hyperstore;

\set ON_ERROR_STOP 0
-- This shows an error but the error is weird, we should probably get
-- a better one.
alter table test2
      set access method hyperstore,
      set (compress_segmentby = 'location_id');
\set ON_ERROR_STOP 1

select relname, amname
  from pg_class join pg_am on (relam = pg_am.oid)
 where relname in ('test2');

-- This will create new chunks for the hypertable
insert into test2 (created_at, location_id, device_id, temp, humidity)
select t, ceil(random()*10), ceil(random()*30), random()*40, random()*100
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5s') t;

-- These should all use the hyperstore access method
select relname, amname
  from show_chunks('test2') as chunk
  join pg_class on (pg_class.oid = chunk)
  join pg_am on (relam = pg_am.oid);

-- Show compression settings for hyperstore across catalog and views
select * from _timescaledb_catalog.compression_settings;
select * from timescaledb_information.compression_settings;
select * from timescaledb_information.chunk_compression_settings;

--------------------------
-- Test alter on chunks --
--------------------------
create table test3 (time timestamptz not null, device int, temp float);
select create_hypertable('test3', 'time');

-- create one chunk
insert into test3 values ('2022-06-01', 1, 1.0);

-- save chunk as variable
select ch as chunk from show_chunks('test3') ch limit 1 \gset

-- Check that chunk is NOT using hyperstore
select relname, amname
  from show_chunks('test3') as chunk
  join pg_class on (pg_class.oid = chunk)
  join pg_am on (relam = pg_am.oid);

\set ON_ERROR_STOP 0
alter table :chunk set access method hyperstore;
\set ON_ERROR_STOP 1

-- Add compression settings
alter table test3 set (timescaledb.compress_segmentby = 'device');
alter table :chunk set access method hyperstore;

-- Check that chunk is using hyperstore
select relname, amname
  from show_chunks('test3') as chunk
  join pg_class on (pg_class.oid = chunk)
  join pg_am on (relam = pg_am.oid);

-- Test setting same access method again
alter table :chunk set access method hyperstore;

-- Create a second chunk
insert into test3 values ('2022-08-01', 1, 1.0);

-- The second chunk should not be a hyperstore chunk
select relname, amname
  from show_chunks('test3') as chunk
  join pg_class on (pg_class.oid = chunk)
  join pg_am on (relam = pg_am.oid);

-- Set hyperstore on hypertable
alter table test3 set access method hyperstore;

-- Create a third chunk
insert into test3 values ('2022-10-01', 1, 1.0);

-- The third chunk should be a hyperstore chunk
select relname, amname
  from show_chunks('test3') as chunk
  join pg_class on (pg_class.oid = chunk)
  join pg_am on (relam = pg_am.oid);
