-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set hypertable readings

\ir hyperstore_helpers.sql

create table :hypertable(
       metric_id serial,
       created_at timestamptz not null unique,
       location_id int,		--segmentby attribute with index
       owner_id int,		--segmentby attribute without index
       device_id int,		--non-segmentby attribute
       temp float,
       humidity float
);

create index hypertable_location_id_idx on :hypertable (location_id);
create index hypertable_device_id_idx on :hypertable (device_id);

select create_hypertable(:'hypertable', by_range('created_at'));
-- Disable incremental sort to make tests stable
set enable_incremental_sort = false;
select setseed(1);

-- Insert rows into the tables.
--
-- The timestamps for the original rows will have timestamps every 10
-- seconds. Any other timestamps are inserted as part of the test.
insert into :hypertable (created_at, location_id, device_id, owner_id, temp, humidity)
select t, ceil(random()*10), ceil(random()*30), ceil(random() * 5), random()*40, random()*100
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '10s') t;

alter table :hypertable set (
	  timescaledb.compress,
	  timescaledb.compress_orderby = 'created_at',
	  timescaledb.compress_segmentby = 'location_id, owner_id'
);


-- Get some test chunks as global variables (first and last chunk here)
select format('%I.%I', chunk_schema, chunk_name)::regclass as chunk1
  from timescaledb_information.chunks
 where format('%I.%I', hypertable_schema, hypertable_name)::regclass = :'hypertable'::regclass
 order by chunk1 asc
 limit 1 \gset

select format('%I.%I', chunk_schema, chunk_name)::regclass as chunk2
  from timescaledb_information.chunks
 where format('%I.%I', hypertable_schema, hypertable_name)::regclass = :'hypertable'::regclass
 order by chunk2 asc
 limit 1 offset 1 \gset
