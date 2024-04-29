-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO queries

\ir include/setup_hyperstore.sql

-- Redefine the indexes to include one value field in the index and
-- check that index-only scans work also for included attributes.
drop index hypertable_location_id_idx;
drop index hypertable_device_id_idx;

create index hypertable_location_id_idx on :hypertable (location_id) include (humidity);
create index hypertable_device_id_idx on :hypertable (device_id) include (humidity);

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

select twist_chunk(show_chunks(:'hypertable'));

vacuum analyze :hypertable;

set max_parallel_workers_per_gather to 0;

-- Test sequence scan
set enable_indexscan to off;
select explain_anonymize(format('select * from %s where owner_id = 3', :'hypertable'));

-- TODO(timescale/timescaledb-private#1117): the Decompress Count here
-- is not correct, but the result shows correctly.
select explain_anonymize(format('select * from %s where owner_id = 3', :'chunk1'));
reset enable_indexscan;

-- Test index scan on non-segmentby column
select explain_anonymize(format($$
   select device_id, avg(temp) from %s where device_id between 10 and 20
   group by device_id
$$, :'hypertable'));

select explain_anonymize(format($$
    select device_id, avg(temp) from %s where device_id between 10 and 20
    group by device_id
$$, :'chunk1'));

-- Test index scan on segmentby column
select explain_anonymize(format($$
    select created_at, location_id, temp from %s where location_id between 5 and 10
$$, :'hypertable'));

select explain_anonymize(format($$
    select created_at, location_id, temp from %s where location_id between 5 and 10
$$, :'chunk1'));

-- These should generate decompressions as above, but for all columns.
select explain_anonymize(format($$
    select * from %s where location_id between 5 and 10
$$, :'hypertable'));

select explain_anonymize(format($$
    select * from %s where location_id between 5 and 10
$$, :'chunk1'));

--
-- Test index only scan
--

create table saved_hypertable as select * from :hypertable;

-- This will not use index-only scan because it is using a segment-by
-- column, but we check that it works as expected though.
--
-- Note that the number of columns decompressed should be zero, since
-- we do not have to decompress any columns.
select explain_anonymize(format($$
    select location_id from %s where location_id between 5 and 10
$$, :'hypertable'));

-- We just compare the counts here, not the full content.
select heapam.count as heapam, hyperstore.count as hyperstore
  from (select count(location_id) from :hypertable where location_id between 5 and 10) heapam,
       (select count(location_id) from :hypertable where location_id between 5 and 10) hyperstore;

drop table saved_hypertable;

-- This should use index-only scan
select explain_anonymize(format($$
    select device_id from %s where device_id between 5 and 10
$$, :'hypertable'));

select explain_anonymize(format($$
    select location_id from %s where location_id between 5 and 10
$$, :'chunk1'));
select explain_anonymize(format($$
    select device_id from %s where device_id between 5 and 10
$$, :'chunk1'));

-- Test index only scan with covering indexes
select explain_anonymize(format($$
    select location_id, avg(humidity) from %s where location_id between 5 and 10
    group by location_id order by location_id
$$, :'hypertable'));

select explain_anonymize(format($$
    select device_id, avg(humidity) from %s where device_id between 5 and 10
    group by device_id order by device_id
$$, :'hypertable'));

select explain_anonymize(format($$
    select location_id, avg(humidity) from %s where location_id between 5 and 10
    group by location_id order by location_id
$$, :'chunk1'));

select explain_anonymize(format($$
    select device_id, avg(humidity) from %s where device_id between 5 and 10
    group by device_id order by device_id
$$, :'chunk1'));

select location_id, round(avg(humidity)) from :hypertable where location_id between 5 and 10
group by location_id order by location_id;

select location_id, round(avg(humidity)) from :chunk1 where location_id between 5 and 10
group by location_id order by location_id;

select device_id, round(avg(humidity)) from :hypertable where device_id between 5 and 10
group by device_id order by device_id;

select device_id, round(avg(humidity)) from :chunk1 where device_id between 5 and 10
group by device_id order by device_id;
