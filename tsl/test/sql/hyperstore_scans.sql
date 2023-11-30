-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

create table readings(time timestamptz, location int, device int, temp float, humidity float);

select create_hypertable('readings', 'time');

select setseed(1);

insert into readings (time, location, device, temp, humidity)
select t, ceil(random()*10), ceil(random()*30), random()*40, random()*100
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5s') t;

alter table readings set (
      timescaledb.compress,
      timescaledb.compress_orderby = 'time',
      timescaledb.compress_segmentby = 'device'
);

select format('%I.%I', chunk_schema, chunk_name)::regclass as chunk
  from timescaledb_information.chunks
 where format('%I.%I', hypertable_schema, hypertable_name)::regclass = 'readings'::regclass
 limit 1 \gset

alter table :chunk set access method tscompression;

--
-- Check that TID scan works for both compressed and non-compressed
-- rows.
--

set timescaledb.enable_transparent_decompression to false;

-- Select any row and try to fetch it using CTID. We do not select the
-- first one just to also try to scan a few rows and make sure the
-- implementation works.
select ctid from :chunk limit 1 offset 10 \gset

explain (costs off)
select * from :chunk where ctid = :'ctid';
select * from :chunk where ctid = :'ctid';

-- Insert a new row, which will then be non-compressed, and fetch it.
insert into :chunk values ('Wed May 25 17:34:56 2022 PDT', 1, 2, 3.14, 2.14);
select ctid from :chunk where time = 'Wed May 25 17:34:56 2022 PDT' \gset

explain (costs off)
select * from :chunk where ctid = :'ctid';
select * from :chunk where ctid = :'ctid';
