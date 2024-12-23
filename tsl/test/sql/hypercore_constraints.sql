-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER;

\ir include/setup_hypercore.sql

-- Drop the unique constraint and replace it with an exclusion
-- constraint doing the same thing.
alter table :hypertable drop constraint readings_created_at_key;
alter table :hypertable add exclude (created_at with =);

create table sample (like :chunk1 including generated including defaults including constraints);
insert into sample(created_at, location_id, device_id, owner_id, temp, humidity)
  values
	('2022-06-01 00:01:23', 999, 666, 111, 3.14, 3.14),
	('2022-06-01 00:02:23', 999, 666, 112, 3.14, 3.14),
	('2022-06-01 00:03:23', 999, 666, 113, 3.14, 3.14),
	('2022-06-01 00:04:23', 999, 666, 114, 3.14, 3.14);

insert into :chunk1(created_at, location_id, device_id, owner_id, temp, humidity)
select created_at, location_id, device_id, owner_id, temp, humidity from sample;

select compress_chunk(show_chunks(:'hypertable'), hypercore_use_access_method => true);

-- These should fail the exclusion constraint
\set ON_ERROR_STOP 0
insert into :hypertable(created_at, location_id, device_id, owner_id, temp, humidity)
select created_at, location_id, device_id, owner_id, temp, humidity from sample;

insert into :chunk1(created_at, location_id, device_id, owner_id, temp, humidity)
select created_at, location_id, device_id, owner_id, temp, humidity from sample;
\set ON_ERROR_STOP 0

create table test_exclude(
       created_at timestamptz not null unique,
       device_id bigint,
       humidity numrange
);

select create_hypertable('test_exclude', by_range('created_at'));

create or replace function randrange() returns numrange as $$
declare
  start numeric := 100.0 * random()::numeric;
begin
  return numrange(start, start + random()::numeric);
end;
$$ language plpgsql;

-- Insert a bunch or rows with a random humidity range.
insert into test_exclude (created_at, device_id, humidity)
select ts, ceil(random()*30), randrange()
from generate_series('2022-06-01'::timestamptz, '2022-07-01', '5m') ts;

-- Pick a chunk to work with.
select exclude_chunk from show_chunks('test_exclude') tbl(exclude_chunk) limit 1 \gset

-- Find all rows that is a duplicate of a previous row.
select * into dups from :exclude_chunk o where (
       select count(*)
       from :exclude_chunk i
       where i.created_at < o.created_at and i.humidity && o.humidity
) > 0;

-- Make sure we have some duplicates. Otherwise, the test does not work.
select count(*) > 0 from dups;

-- Delete the duplicates.
delete from :exclude_chunk where created_at in (select created_at from dups);

-- Add an exclusion constraint.
alter table :exclude_chunk add constraint humidity_overlap exclude using gist (humidity with &&);

-- Make sure that inserting some duplicate fails on this the exclusion constraint.
\set ON_ERROR_STOP 0
insert into :exclude_chunk select * from dups limit 10;
insert into test_exclude select * from dups limit 10;
\set ON_ERROR_STOP 1
