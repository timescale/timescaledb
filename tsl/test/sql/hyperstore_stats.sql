-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hyperstore.sql

-- To generate plans consistently.
set max_parallel_workers_per_gather to 0;

-- Start without these indexes
drop index hypertable_location_id_idx;
drop index hypertable_device_id_idx;

--
-- Test ANALYZE.
--
-- First create a separate regular table with the chunk data as a
-- reference of accurate stats. Analyze it and compare with analyze
-- on the original chunk.
--
create table normaltable (like :chunk1);
insert into normaltable select * from :chunk1;

create view relstats as
select oid::regclass as relid, reltuples
from pg_class
order by relid;

create view relstats_compare as
select * from relstats where relid in ('normaltable'::regclass, :'chunk1'::regclass);

create view attrstats as
select format('%I.%I', schemaname, tablename)::regclass as relid,
       attname,
       n_distinct,
       array_to_string(most_common_vals, e',') as most_common_vals
from pg_stats
order by relid, attname;

create view attrstats_compare as
select * from attrstats where relid in ('normaltable'::regclass, :'chunk1'::regclass);

create view attrstats_diff as
select attname, n_distinct, most_common_vals from attrstats where relid = 'normaltable'::regclass
except
select attname, n_distinct, most_common_vals from attrstats where relid = :'chunk1'::regclass;

create view attrstats_same as
select count(*)=0 as stats_are_the_same from attrstats_diff;

-- No stats yet
select * from relstats_compare;
select * from attrstats_compare;

-- Check that the estimated rows is the same for the chunk and the
-- normal table and in the right ballpark
explain (analyze, timing off, summary off)
select * from :chunk1 where location_id = 1;
explain (analyze, timing off, summary off)
select * from normaltable where location_id = 1;

-- Changing to hyperstore will update relstats since it process all
-- the data
alter table :chunk1 set access method hyperstore;
-- Creating an index on normaltable will also update relstats
create index normaltable_location_id_idx on normaltable (location_id);

-- Relstats should be the same for both tables, except for pages since
-- a hyperstore is compressed. Column stats is not updated.
select * from relstats_compare;
select * from attrstats_compare;

-- Drop the index again, we'll need to recreate it again later
drop index normaltable_location_id_idx;

-- Check that the estimated rows is the same for the chunk and the
-- normal table and in the right ballpark
select count(*) from :chunk1 where location_id = 1;
explain (analyze, timing off, summary off)
select * from :chunk1 where location_id = 1;
explain (analyze, timing off, summary off)
select * from normaltable where location_id = 1;

-- ANALYZE directly on chunk
analyze :chunk1;
analyze normaltable;

-- Stats after analyze
select * from relstats_compare;
select * from attrstats_same;

-- Check that the estimated rows is now correct based on stats (reltuples)
explain (analyze, timing off, summary off)
select * from :chunk1 where location_id = 1;
explain (analyze, timing off, summary off)
select * from normaltable where location_id = 1;

delete from :chunk1 where location_id=1;
delete from normaltable where location_id=1;

select * from relstats_compare;
select * from attrstats_same;

-- Creating an index should update relstats but not attrstats. Note
-- also that this will be a segmentby index, and because it only
-- indexes compressed tuples, it is extra important to check reltuples
-- is correct.
create index hypertable_location_id_idx on :hypertable (location_id);
create index normaltable_location_id_idx on normaltable (location_id);

-- Test how VACUUM affects stats (or not)
select * from relstats_compare;

vacuum :chunk1;
vacuum normaltable;

select * from relstats_compare;
select * from attrstats_same;

vacuum analyze :chunk1;
vacuum analyze normaltable;

select * from relstats_compare;
select * from attrstats_same;

-- ANALYZE also via hypertable root and show that it will recurse to
-- chunks. Make sure the chunk also has partially compressed data
alter table :chunk2 set access method hyperstore;
update :hypertable set device_id = 2 where device_id = 1;
select * from relstats where relid = :'chunk2'::regclass;
select * from attrstats where relid = :'chunk2'::regclass;
select count(*) from :chunk2;

analyze :hypertable;

select * from relstats where relid = :'chunk2'::regclass;
-- Just show that there are attrstats via a count avoid flaky output
select count(*) from attrstats where relid = :'chunk2'::regclass;
