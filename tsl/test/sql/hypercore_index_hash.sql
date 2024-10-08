-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/setup_hypercore.sql

-- Set the number of parallel workers to zero to disable parallel
-- plans. This differs between different PG versions.
set max_parallel_workers_per_gather to 0;

-- Redefine the indexes to use hash indexes
drop index hypertable_location_id_idx;
drop index hypertable_device_id_idx;

-- Discourage sequence scan when there are alternatives to avoid flaky
-- tests.
set enable_seqscan to false;

create index hypertable_location_id_idx on :hypertable using hash (location_id);
create index hypertable_device_id_idx on :hypertable using hash (device_id);

create view chunk_indexes as
select ch::regclass as chunk, indexrelid::regclass as index, attname
from pg_attribute att inner join pg_index ind
on (att.attrelid=ind.indrelid and att.attnum=ind.indkey[0])
inner join show_chunks(:'hypertable') ch on (ch = att.attrelid)
order by chunk, index;

-- save some reference data from an index (only) scan
select explain_anonymize(format($$
       select location_id, count(*) into orig from %s
       where location_id in (3,4,5) group by location_id
$$, :'hypertable'));

select location_id, count(*) into orig from :hypertable
where location_id in (3,4,5) group by location_id;

alter table :chunk2 set access method hypercore;

--
-- test that indexes work after updates
--

-- find a compressed tuple in a deterministic manner and get location and timestamp
select created_at, location_id
from :chunk2 where _timescaledb_debug.is_compressed_tid(ctid)
order by created_at, location_id limit 1 \gset

select _timescaledb_debug.is_compressed_tid(ctid), *
  from :chunk2 where location_id = :location_id and created_at = :'created_at';

-- first update moves the value from the compressed rel to the non-compressed (seen via ctid)
update :hypertable set temp=1.0 where location_id=:location_id and created_at=:'created_at';

select _timescaledb_debug.is_compressed_tid(ctid), *
  from :chunk2 where location_id = :location_id and created_at = :'created_at';

-- second update should be a hot update (tuple in same block after update, as shown by ctid)
update :hypertable set temp=2.0 where location_id=:location_id and created_at=:'created_at';

select _timescaledb_debug.is_compressed_tid(ctid), *
  from :chunk2 where location_id = :location_id and created_at = :'created_at';

-- make sure query uses a segmentby index and returns the correct data for the update value
select explain_anonymize(format($$
       select created_at, location_id, temp from %s where location_id=1 and temp=2.0
$$, :'chunk2'));
select created_at, location_id, temp from :chunk2 where location_id=1 and temp=2.0;

select compress_chunk(show_chunks(:'hypertable'), compress_using => 'hypercore');

vacuum analyze :hypertable;

-- Test index scan on non-segmentby column
select explain_analyze_anonymize(format($$
   select device_id, avg(temp) from %s where device_id = 10
   group by device_id
$$, :'hypertable'));

select explain_analyze_anonymize(format($$
   select device_id, avg(temp) from %s where device_id = 10
   group by device_id
$$, :'chunk1'));

-- Test index scan on segmentby column
select explain_analyze_anonymize(format($$
    select created_at, location_id, temp from %s where location_id = 5
$$, :'hypertable'));

select explain_analyze_anonymize(format($$
    select created_at, location_id, temp from %s where location_id = 5
$$, :'chunk1'));

-- These should generate decompressions as above, but for all columns.
select explain_analyze_anonymize(format($$
    select * from %s where location_id = 5
$$, :'hypertable'));

select explain_analyze_anonymize(format($$
    select * from %s where location_id = 5
$$, :'chunk1'));

drop table :hypertable cascade;
