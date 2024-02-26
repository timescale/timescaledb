-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hyperstore.sql

create view chunk_indexes as
select ch::regclass as chunk, indexrelid::regclass as index, attname
from pg_attribute att inner join pg_index ind
on (att.attrelid=ind.indrelid and att.attnum=ind.indkey[0])
inner join show_chunks(:'hypertable') ch on (ch = att.attrelid)
order by chunk, index;

-- save some reference data from an index (only) scan
explain (costs off)
select location_id, count(*) into orig from :hypertable
where location_id in (3,4,5) group by location_id;

select location_id, count(*) into orig from :hypertable
where location_id in (3,4,5) group by location_id;

-- show index size before switching to hyperstore
select index, pg_relation_size(index)
from chunk_indexes
where chunk=:'chunk2'::regclass and (attname='location_id' or attname='device_id' or attname='owner_id');

alter table :chunk2 set access method hyperstore;

-- count without indexes
select owner_id, count(*) into owner_orig from :hypertable
where owner_id in (3,4,5) group by owner_id;

-- create indexes on all segmentby columns
create index hypertable_owner_idx on :hypertable (owner_id);
create index hypertable_location_id_owner_id_idx on :hypertable (location_id, owner_id);

-- Result should be the same with indexes
explain (costs off)
select owner_id, count(*) into owner_comp from :hypertable
where owner_id in (3,4,5) group by owner_id;
select owner_id, count(*) into owner_comp from :hypertable
where owner_id in (3,4,5) group by owner_id;
select * from owner_orig join owner_comp using (owner_id) where owner_orig.count != owner_comp.count;


-- the indexes on segmentby columns should be smaller on hyperstore
-- while the device_id index remains the same size
select index, pg_relation_size(index)
from chunk_indexes
where chunk=:'chunk2'::regclass and (attname='location_id' or attname='device_id' or attname='owner_id');

-- the query should not use index-only scan on the hypestore chunk
-- (number 2) because it is not supported on segmentby indexes
explain (costs off)
select location_id, count(*) into comp from :hypertable
where location_id in (3,4,5) group by location_id;

-- result should be the same
select location_id, count(*) into comp from :hypertable where location_id in (3,4,5) group by location_id;
select * from orig join comp using (location_id) where orig.count != comp.count;

--
-- test that indexes work after updates
--
select ctid, created_at, location_id, temp from :chunk2 order by location_id, created_at desc limit 2;

-- first update moves the value from the compressed rel to the non-compressed (seen via ctid)
update :hypertable set temp=1.0 where location_id=1 and created_at='Wed Jun 08 16:57:50 2022 PDT';
select ctid, created_at, location_id, temp from :chunk2 order by location_id, created_at desc limit 2;

-- second update should be a hot update (tuple in same block after update, as shown by ctid)
update :hypertable set temp=2.0 where location_id=1 and created_at='Wed Jun 08 16:57:50 2022 PDT';
select ctid, created_at, location_id, temp from :chunk2 order by location_id, created_at desc limit 2;

-- make sure query uses a segmentby index and returns the correct data for the update value
explain (costs off)
select created_at, location_id, temp from :chunk2 where location_id=1 and temp=2.0;
select created_at, location_id, temp from :chunk2 where location_id=1 and temp=2.0;
