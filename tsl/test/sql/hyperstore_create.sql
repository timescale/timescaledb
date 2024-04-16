-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Testing the basic API for creating a hyperstore

-- This is a replacement for compress chunk for the time being.
create function twist_chunk(chunk regclass) returns regclass language plpgsql
as $$
begin
    execute format('alter table %s set access method hyperstore', chunk);
    return chunk;
end
$$;

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
